# =============================================================================
# BOT POLYMARKET — PURE QUANT MARKET MAKER v3.0
# =============================================================================
# NOVIDADES v3.0:
#   ✅ LIVE = True/False  — interruptor único paper/live
#   ✅ Credenciais lidas de secrets.txt (nunca hardcoded)
#   ✅ Em LIVE: lê saldo USDC real da carteira Polygon on-chain
#   ✅ Em LIVE: posta limit orders GTC reais via py-clob-client
#   ✅ Em LIVE: cancela ordens antigas antes de cada quote
#   ✅ Bankroll inicial: $10 (lido da carteira em LIVE)
#   ✅ Loop INFINITO: ronda após ronda do mercado XRP 5-min
#      - Quando um mercado fecha, encontra o próximo automaticamente
#      - Nunca termina — usa Ctrl+C para parar
#
# INSTALAR DEPENDÊNCIAS:
#   pip install py-clob-client web3 numpy requests
#
# ESTRUTURA DO secrets.txt:
#   PRIVATE_KEY=0x...
#   API_KEY=...
#   API_SECRET=...
#   API_PASSPHRASE=...
# =============================================================================

import asyncio
import time
import math
import os
import logging
import logging.handlers
import numpy as np
from collections import deque
from dataclasses import dataclass
from typing import Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from py_clob_client.client import ClobClient as _ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType, ApiCreds
    from py_clob_client.constants import POLYGON
    HAS_CLOB = True
except ImportError:
    HAS_CLOB = False

try:
    from web3 import Web3
    HAS_WEB3 = True
except ImportError:
    HAS_WEB3 = False

# =============================================================================
# INTERRUPTOR PRINCIPAL
# =============================================================================

LIVE = False   # ← Muda para True para ir a mercado real

# =============================================================================
# LOGGING — FICHEIRO pure_quant_market_maker.log
# =============================================================================

LOG_FILE = "pure_quant_market_maker.log"

def setup_logging():
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    root.addHandler(fh)

setup_logging()
log = logging.getLogger("quant_mm")

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

@dataclass
class MMConfig:
    # ── Banca ─────────────────────────────────────────────────────────────────
    initial_bankroll:        float = 10.0      # $10 — ignorado em LIVE (lê carteira)

    # ── Kelly ─────────────────────────────────────────────────────────────────
    kelly_mc_paths:          int   = 2_000
    kelly_confidence:        float = 0.10
    kelly_min_history:       int   = 20
    kelly_max_fraction:      float = 0.08
    kelly_half_threshold:    float = 0.03

    # ── Avellaneda-Stoikov ────────────────────────────────────────────────────
    gamma:                   float = 0.15
    kappa_default:           float = 1.5
    kappa_min_to_quote:      float = 0.5
    sigma_window:            int   = 100

    # ── VPIN ──────────────────────────────────────────────────────────────────
    vpin_bucket_size:        int   = 25
    vpin_n_buckets:          int   = 50
    vpin_widen_threshold:    float = 0.50
    vpin_throttle_threshold: float = 0.65
    vpin_withdraw_threshold: float = 0.80

    # ── Fees Polymarket 5-min crypto ──────────────────────────────────────────
    fee_rate:                float = 0.25
    fee_exponent:            float = 2.0
    maker_rebate_pct:        float = 0.20

    # ── Spread ────────────────────────────────────────────────────────────────
    min_spread_cents:        float = 1.5
    max_spread_cents:        float = 10.0

    # ── Inventário ────────────────────────────────────────────────────────────
    max_inventory_frac:      float = 0.25
    emergency_unwind_frac:   float = 0.35

    # ── Operacional ───────────────────────────────────────────────────────────
    tick_interval_s:         float = 2.0       # segundos entre ticks
    between_markets_wait_s:  float = 10.0      # espera entre rondas
    market_duration_s:       float = 300.0     # 5 minutos por ronda

    # ── Polymarket Endpoints ──────────────────────────────────────────────────
    clob_host:               str   = "https://clob.polymarket.com"
    gamma_host:              str   = "https://gamma-api.polymarket.com"
    polygon_rpc:             str   = "https://polygon-rpc.com"
    usdc_contract:           str   = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

cfg = MMConfig()

# =============================================================================
# LEITURA DE SECRETS
# =============================================================================

def load_secrets(path: str = "secrets.txt") -> dict:
    """
    Lê credenciais do secrets.txt.
    Formato:
        PRIVATE_KEY=0x...
        API_KEY=...
        API_SECRET=...
        API_PASSPHRASE=...
    """
    if not os.path.exists(path):
        log.error(f"[SECRETS] '{path}' nao encontrado em {os.path.abspath(path)}")
        raise FileNotFoundError(path)

    secrets = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                secrets[k.strip()] = v.strip()

    required = ["PRIVATE_KEY", "API_KEY", "API_SECRET", "API_PASSPHRASE"]
    missing  = [k for k in required if k not in secrets]
    if missing:
        raise ValueError(f"Chaves em falta no secrets.txt: {missing}")

    log.info(f"[SECRETS] Carregadas (API_KEY: ...{secrets['API_KEY'][-6:]})")
    return secrets

# =============================================================================
# FEE ENGINE
# =============================================================================

class FeeEngine:
    @staticmethod
    def taker_fee(shares: float, price: float) -> float:
        return shares * price * cfg.fee_rate * (price * (1.0 - price)) ** cfg.fee_exponent

    @staticmethod
    def maker_rebate(shares: float, price: float) -> float:
        return FeeEngine.taker_fee(shares, price) * cfg.maker_rebate_pct

    @staticmethod
    def eff_pct(price: float) -> float:
        return (FeeEngine.taker_fee(100.0, price) / (100.0 * price) * 100.0) if price > 0 else 0.0

    @staticmethod
    def min_spread(price: float) -> float:
        return max(cfg.min_spread_cents / 100.0, FeeEngine.eff_pct(price) / 100.0 * 0.4)

# =============================================================================
# WALLET — SALDO REAL DA CARTEIRA POLYMARKET
# =============================================================================

class WalletManager:
    """
    Em LIVE: lê o saldo USDC real on-chain (Polygon).
    Em PAPER: devolve cfg.initial_bankroll ($10).
    """
    def __init__(self, private_key: Optional[str] = None):
        self.address = None
        self._w3     = None

        if LIVE and HAS_WEB3 and private_key and private_key != "0xSIM":
            try:
                self._w3    = Web3(Web3.HTTPProvider(cfg.polygon_rpc))
                acct        = self._w3.eth.account.from_key(private_key)
                self.address = acct.address
                log.info(f"[WALLET] Endereco: {self.address}")
            except Exception as e:
                log.error(f"[WALLET] Erro ao derivar endereco: {e}")

    def get_balance_usdc(self) -> float:
        if not LIVE:
            return cfg.initial_bankroll

        if not self._w3 or not self.address:
            log.warning("[WALLET] Web3 indisponivel, usando saldo inicial.")
            return cfg.initial_bankroll

        try:
            abi = [{"inputs":[{"name":"account","type":"address"}],
                    "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
                    "stateMutability":"view","type":"function"}]
            usdc    = self._w3.eth.contract(
                        address=Web3.to_checksum_address(cfg.usdc_contract), abi=abi)
            raw     = usdc.functions.balanceOf(self.address).call()
            balance = raw / 1_000_000  # USDC tem 6 casas decimais
            log.info(f"[WALLET] Saldo USDC on-chain: ${balance:.4f}")
            return balance
        except Exception as e:
            log.error(f"[WALLET] Erro ao ler saldo: {e}")
            return cfg.initial_bankroll

# =============================================================================
# POLYMARKET CLOB — LIMIT ORDERS REAIS
# =============================================================================

class PolymarketCLOB:
    """
    Em PAPER: simula ordens (log only).
    Em LIVE:  assina e envia limit orders reais via py-clob-client.

    Limit Order Flow por tick:
      1. cancel_all()  — cancela ordens pendentes anteriores
      2. post_limit()  — posta GTC limit order no BID
      3. post_limit()  — posta GTC limit order no ASK
    """
    def __init__(self, secrets: dict):
        self._client       = None
        self._active_ids:  list = []

        if LIVE and HAS_CLOB:
            try:
                creds = ApiCreds(
                    api_key        = secrets["API_KEY"],
                    api_secret     = secrets["API_SECRET"],
                    api_passphrase = secrets["API_PASSPHRASE"],
                )
                self._client = _ClobClient(
                    host     = cfg.clob_host,
                    key      = secrets["PRIVATE_KEY"],
                    chain_id = POLYGON,
                    creds    = creds,
                )
                log.info("[CLOB] Cliente LIVE inicializado.")
            except Exception as e:
                log.error(f"[CLOB] Falha ao inicializar: {e}")
        else:
            log.info(f"[CLOB] Modo: {'LIVE sem py-clob-client' if LIVE else 'PAPER'}")

    async def cancel_all(self, token_id: str):
        if not self._active_ids:
            return
        if LIVE and self._client:
            try:
                self._client.cancel_orders(self._active_ids)
                log.info(f"[CLOB][LIVE] {len(self._active_ids)} ordens canceladas.")
            except Exception as e:
                log.warning(f"[CLOB] Erro ao cancelar: {e}")
        self._active_ids.clear()

    async def post_limit_order(self, token_id: str, side: str,
                               price: float, size_usdc: float) -> Optional[str]:
        shares = round(size_usdc / price, 2) if price > 0 else 0

        if LIVE and self._client:
            try:
                args = OrderArgs(
                    token_id   = token_id,
                    price      = round(price, 4),
                    size       = shares,
                    side       = side,
                    order_type = OrderType.GTC,
                )
                resp     = self._client.create_and_post_order(args)
                order_id = resp.get("orderID") or resp.get("order_id", "unknown")
                self._active_ids.append(order_id)
                log.info(f"[CLOB][LIVE] {side} {shares:.2f}sh @ {price*100:.1f}c "
                         f"(${size_usdc:.2f}) → {order_id[:16]}...")
                return order_id
            except Exception as e:
                log.error(f"[CLOB] Erro ao postar {side}: {e}")
                return None
        else:
            oid = f"sim_{side}_{time.time():.3f}"
            self._active_ids.append(oid)
            log.info(f"[CLOB][SIM] {side} {shares:.2f}sh @ {price*100:.1f}c "
                     f"(${size_usdc:.2f}) → {oid[-10:]}")
            return oid

    async def emergency_sell(self, token_id: str, shares: float):
        await self.cancel_all(token_id)
        if LIVE and self._client:
            try:
                args = OrderArgs(token_id=token_id, price=0.01,
                                 size=round(shares, 2), side="SELL",
                                 order_type=OrderType.FOK)
                self._client.create_and_post_order(args)
                log.error(f"[CLOB][LIVE] EMERGENCY SELL {shares:.2f}sh executado.")
            except Exception as e:
                log.error(f"[CLOB] Erro no emergency sell: {e}")
        else:
            log.error(f"[CLOB][SIM] EMERGENCY SELL {shares:.2f}sh simulado.")

    async def get_fills(self, token_id: str) -> list:
        if LIVE and self._client:
            try:
                trades = self._client.get_trades({"maker": True, "token_id": token_id})
                return trades.get("data", [])
            except Exception as e:
                log.warning(f"[CLOB] Erro ao buscar fills: {e}")
        return []

# =============================================================================
# MARKET FINDER — Encontra próximo mercado XRP 5-min
# =============================================================================

class MarketFinder:
    @staticmethod
    def find_xrp_5min() -> Optional[dict]:
        if not LIVE or not HAS_REQUESTS:
            return MarketFinder._sim_market()

        try:
            resp = requests.get(
                f"{cfg.gamma_host}/markets",
                params={"active": "true", "closed": "false",
                        "tag": "crypto", "limit": 100},
                timeout=10
            )
            resp.raise_for_status()
            markets = resp.json()

            xrp_mkts = []
            for m in markets:
                txt = (m.get("slug","") + m.get("question","")).lower()
                if "xrp" in txt and ("5" in txt):
                    xrp_mkts.append(m)

            if not xrp_mkts:
                log.warning("[MARKET] Sem mercado XRP 5-min activo.")
                return None

            import datetime
            now = time.time()
            def get_end(m):
                s = m.get("endDateIso") or m.get("end_date_iso","")
                try:
                    return datetime.datetime.fromisoformat(
                        s.replace("Z","+00:00")).timestamp()
                except:
                    return now + 9999

            xrp_mkts.sort(key=get_end)
            for m in xrp_mkts:
                t_rem = get_end(m) - now
                if t_rem > 30:
                    log.info(f"[MARKET] {m.get('question','?')} | T-rem: {t_rem:.0f}s")
                    m["_end_ts"] = get_end(m)
                    return m
        except Exception as e:
            log.error(f"[MARKET] Erro API: {e}")
        return None

    @staticmethod
    def _sim_market() -> dict:
        return {
            "conditionId": "0xSIM_XRP_MARKET_" + str(int(time.time())),
            "question":    "[SIM] XRP above $2.50 in 5min?",
            "tokenIds":    ["SIM_TOKEN_YES", "SIM_TOKEN_NO"],
            "_end_ts":     time.time() + cfg.market_duration_s,
        }

    @staticmethod
    def token_id(market: dict) -> str:
        tokens = market.get("tokenIds") or []
        return tokens[0] if tokens else market.get("conditionId","")

# =============================================================================
# MODELOS QUANT
# =============================================================================

class KellyEngine:
    def __init__(self):
        self.returns: list  = []
        self.peak_bk: float = cfg.initial_bankroll
        self.cur_bk:  float = cfg.initial_bankroll

    def record(self, pnl_pct: float, new_bk: float):
        self.returns.append(pnl_pct)
        self.cur_bk  = new_bk
        self.peak_bk = max(self.peak_bk, new_bk)

    @property
    def dd(self):
        return (self.peak_bk - self.cur_bk) / self.peak_bk if self.peak_bk else 0.0

    @property
    def wr(self):
        return sum(1 for r in self.returns if r>0)/len(self.returns) if self.returns else 0.0

    def fraction(self) -> float:
        if len(self.returns) < cfg.kelly_min_history:
            return cfg.kelly_max_fraction * 0.10
        r  = np.array(self.returns)
        fg = np.array([np.prod(1+np.random.choice(r,size=len(r),replace=True))
                       for _ in range(cfg.kelly_mc_paths)])
        pg = np.percentile(fg, cfg.kelly_confidence*100)
        mr, vr = np.mean(r), np.var(r)
        if mr <= 0 or vr == 0: return cfg.kelly_max_fraction*0.10
        f = mr / (mr**2 + vr)
        if self.dd > cfg.kelly_half_threshold:
            f *= 0.5
            log.info(f"[KELLY] DD {self.dd:.1%} → Half-Kelly (f={f:.4f})")
        if pg < 1.0: f *= pg
        return float(np.clip(f, 1e-4, cfg.kelly_max_fraction))


class VPINEngine:
    def __init__(self):
        self._b=self._s=self._v=0.0
        self._bkts: deque = deque(maxlen=cfg.vpin_n_buckets)

    def add(self, volume: float, is_buy: bool):
        if is_buy: self._b+=volume
        else:      self._s+=volume
        self._v+=volume
        if self._v >= cfg.vpin_bucket_size:
            self._bkts.append(abs(self._b-self._s)/self._v)
            self._b=self._s=self._v=0.0

    @property
    def value(self): return float(np.mean(list(self._bkts))) if len(self._bkts)>=3 else 0.0

    @property
    def regime(self):
        v=self.value
        if v>=cfg.vpin_withdraw_threshold: return "TOXIC"
        if v>=cfg.vpin_throttle_threshold: return "THROTTLE"
        if v>=cfg.vpin_widen_threshold:    return "WIDEN"
        return "NORMAL"


class KappaEstimator:
    def __init__(self): self._t: deque = deque(maxlen=500)
    def record(self): self._t.append(time.time())
    def estimate(self):
        if len(self._t)<10: return cfg.kappa_default
        t=list(self._t); w=t[-1]-t[0]
        return float(np.clip(len(t)/w,0.1,20.0)) if w>0 else cfg.kappa_default


class ASStoikovPricer:
    def __init__(self):
        self._px: deque = deque(maxlen=cfg.sigma_window)
        self.inventory  = 0.0
        self.kappa      = KappaEstimator()
        self.vpin       = VPINEngine()

    def reset(self):
        self.inventory=0.0; self._px.clear()
        log.info("[PRICER] Inventario resetado para nova ronda.")

    def add_tick(self, price: float, volume: float, is_buy: bool):
        self._px.append(price); self.kappa.record(); self.vpin.add(volume,is_buy)

    @property
    def sigma2(self):
        if len(self._px)<5: return 0.0025
        p=np.array(list(self._px))
        return float(np.var(np.diff(p)/(p[:-1]+1e-9)))+1e-9

    def quote(self, mid: float, t_rem: float, bankroll: float) -> dict:
        reg=self.vpin.regime; vv=self.vpin.value
        k=self.kappa.estimate(); s2=self.sigma2; mx=bankroll*cfg.max_inventory_frac

        if reg=="TOXIC":      return {"status":"WITHDRAW","vpin":vv,"reason":"TOXIC_FLOW"}
        if k<cfg.kappa_min_to_quote: return {"status":"WITHDRAW","vpin":vv,"reason":"DRY_MARKET"}
        if abs(self.inventory)>=bankroll*cfg.emergency_unwind_frac:
            return {"status":"EMERGENCY_UNWIND","inventory":self.inventory,"vpin":vv}

        rp = mid-(self.inventory*cfg.gamma*s2*t_rem)
        sp = cfg.gamma*s2*t_rem + (2/cfg.gamma)*math.log(1+cfg.gamma/k) + 2*0.3*vv*mid

        if reg=="WIDEN":      sp*=1.5
        elif reg=="THROTTLE": sp*=2.5

        ms=FeeEngine.min_spread(mid)
        sp=float(np.clip(sp,ms,cfg.max_spread_cents/100))
        h=sp/2; tilt=h*(self.inventory/(mx+1e-9))*0.5
        bid=float(np.clip(rp-h+tilt,0.01,0.99))
        ask=float(np.clip(rp+h+tilt,0.01,0.99))
        if ask<=bid+ms: ask=bid+ms

        return {"status":"QUOTE","bid":bid,"ask":ask,"spread":ask-bid,
                "vpin":vv,"regime":reg,"kappa":k,"sigma2":s2,
                "fee_pct":FeeEngine.eff_pct(mid),
                "rebate_ask":FeeEngine.maker_rebate(1.0,ask)}

# =============================================================================
# PnL TRACKER
# =============================================================================

class PnLTracker:
    def __init__(self, bk0: float):
        self.bankroll=bk0; self.spr=self.reb=self.adv=0.0
        self.nf=self.nw=self.nl=self.n_rounds=0
        self._hist: deque = deque(maxlen=1000)
        self._t0=time.time(); self._bk0=bk0

    def record_fill(self, fp: float, sz_usdc: float, mid: float, rebate: float):
        shares   = sz_usdc/(fp+1e-9)
        sc       = abs(fp-mid)*shares
        adv      = math.sqrt(0.0025)*0.5*shares*0.3
        pnl      = sc+rebate-adv
        self.spr+=sc; self.reb+=rebate; self.adv-=adv
        self.bankroll+=pnl; self.nf+=1
        if pnl>0: self.nw+=1
        else:     self.nl+=1
        self._hist.append(pnl)

    @property
    def pnl_pct(self): return (self.bankroll-self._bk0)/self._bk0
    @property
    def wr(self):
        t=self.nw+self.nl; return self.nw/t if t else 0.0
    @property
    def sharpe(self):
        if len(self._hist)<10: return 0.0
        p=np.array(list(self._hist))
        return float((p.mean()/(p.std()+1e-9))*math.sqrt(len(p)))

    def log_round(self, rn: int):
        el=time.time()-self._t0
        log.info("─"*62)
        log.info(f"RONDA {rn} TERMINADA | Bankroll: ${self.bankroll:.4f} ({self.pnl_pct:>+.2%})")
        log.info(f"Spread PnL: ${self.spr:>+.4f} | Rebate: ${self.reb:>+.4f} | Adverse: ${self.adv:>+.4f}")
        log.info(f"Win Rate: {self.wr:.1%} ({self.nw}W/{self.nl}L) | Fills: {self.nf} | Sharpe: {self.sharpe:.2f}")
        log.info(f"Runtime: {el/60:.1f}min | {self.n_rounds} rondas completas")
        log.info("─"*62)

# =============================================================================
# PAPER SIMULATOR
# =============================================================================

class PaperSim:
    def __init__(self, mid=0.50):
        self._mid=mid; self._reg="NORMAL"; self._tl=0

    def tick(self):
        if self._tl<=0:
            self._reg=np.random.choice(["NORMAL","INFORMED"],p=[0.85,0.15])
            self._tl=np.random.randint(10,60)
            if self._reg=="INFORMED":
                log.warning(f"[SIM] *** REGIME INFORMED *** {self._tl} ticks | VPIN vai subir")
        self._tl-=1
        self._mid=float(np.clip(self._mid+np.random.normal(0,0.001),0.05,0.95))
        v=float(np.random.exponential(1)+0.5)
        ib=True if self._reg=="INFORMED" else bool(np.random.choice([True,False]))
        return self._mid,v,ib

    def try_fill(self, bid: float, ask: float, sz_usdc: float):
        if np.random.random()>0.30: return None
        side=np.random.choice(["BUY","SELL"])
        fp=bid if side=="BUY" else ask
        sz=sz_usdc*np.random.uniform(0.2,1.0)
        return side,fp,sz

# =============================================================================
# LIVE MID PRICE
# =============================================================================

async def get_mid(clob: PolymarketCLOB, token_id: str,
                  sim: PaperSim) -> tuple:
    if not LIVE or not clob._client:
        return sim.tick()
    try:
        book=clob._client.get_order_book(token_id)
        bids=book.get("bids",[]); asks=book.get("asks",[])
        if bids and asks:
            mid=(float(bids[0]["price"])+float(asks[0]["price"]))/2
            return mid,1.0,mid<0.50
    except Exception as e:
        log.warning(f"[LIVE] Order book erro: {e}")
    return sim.tick()

# =============================================================================
# UMA RONDA
# =============================================================================

async def run_round(rn: int, market: dict, clob: PolymarketCLOB,
                    kelly: KellyEngine, pricer: ASStoikovPricer,
                    pnl: PnLTracker, sim: PaperSim):
    token     = MarketFinder.token_id(market)
    end_ts    = market.get("_end_ts", time.time()+cfg.market_duration_s)
    question  = market.get("question","?")

    log.info("="*62)
    log.info(f"RONDA {rn} | {question}")
    log.info(f"Token: {token[:50]}")
    log.info(f"Bankroll: ${pnl.bankroll:.4f} | Mode: {'LIVE' if LIVE else 'PAPER'}")
    log.info("="*62)

    pricer.reset()
    tick_n=0

    while True:
        now=time.time(); t_rem=end_ts-now
        if t_rem<=0:
            log.info(f"[RONDA {rn}] Mercado fechado.")
            break

        tick_n+=1
        mid,vol,ib=await get_mid(clob,token,sim)
        pricer.add_tick(mid,vol,ib)
        await clob.cancel_all(token)

        result=pricer.quote(mid=mid,t_rem=t_rem,bankroll=pnl.bankroll)

        if result["status"]=="EMERGENCY_UNWIND":
            log.error(f"[T-{t_rem:>5.1f}s] *** EMERGENCY UNWIND *** "
                      f"Inv={result['inventory']:+.1f} VPIN={result['vpin']:.2f}")
            await clob.emergency_sell(token,abs(pricer.inventory))
            pricer.inventory=0.0
            await asyncio.sleep(cfg.tick_interval_s); continue

        if result["status"]=="WITHDRAW":
            log.warning(f"[T-{t_rem:>5.1f}s] WITHDRAW | {result['reason']} | VPIN={result['vpin']:.2f}")
            await asyncio.sleep(cfg.tick_interval_s); continue

        fk=kelly.fraction()
        if result["regime"]=="THROTTLE": fk*=0.4
        sz=float(np.clip(pnl.bankroll*fk, 0.10, pnl.bankroll*cfg.max_inventory_frac))

        # Limit orders GTC no BID e ASK
        await clob.post_limit_order(token,"BUY", result["bid"],sz)
        await clob.post_limit_order(token,"SELL",result["ask"],sz)

        # Fills
        if LIVE:
            for fill in await clob.get_fills(token):
                fp=float(fill.get("price",mid))
                sz_u=float(fill.get("size",0))*fp
                reb=FeeEngine.maker_rebate(float(fill.get("size",0)),fp)
                side=fill.get("side","BUY")
                pnl.record_fill(fp,sz_u,mid,reb)
                kelly.record((fp-mid)/(mid+1e-9),pnl.bankroll)
                pricer.inventory+=sz_u/fp if side=="BUY" else -sz_u/fp
        else:
            f=sim.try_fill(result["bid"],result["ask"],sz)
            if f:
                side,fp,sz_u=f
                reb=FeeEngine.maker_rebate(sz_u/(fp+1e-9),fp)
                pnl.record_fill(fp,sz_u,mid,reb)
                kelly.record((fp-mid)/(mid+1e-9),pnl.bankroll)
                pricer.inventory+=sz_u/fp if side=="BUY" else -sz_u/fp
                log.info(f"[T-{t_rem:>5.1f}s] FILL {side:4s} | "
                         f"{fp*100:.1f}c | ${sz_u:.4f} | Reb=${reb:.5f} | "
                         f"Inv={pricer.inventory:>+5.1f} | Bk=${pnl.bankroll:.4f} | "
                         f"PnL={pnl.pnl_pct:>+.3%}")

        if tick_n%5==0:
            log.info(f"[T-{t_rem:>5.1f}s] QUOTE | "
                     f"Mid={mid*100:>5.1f}c | "
                     f"Bid={result['bid']*100:>5.1f}c/Ask={result['ask']*100:>5.1f}c | "
                     f"Spr={result['spread']*100:.2f}c | "
                     f"Fee={result['fee_pct']:.2f}% | "
                     f"VPIN={result['vpin']:.2f}[{result['regime']:8s}] | "
                     f"k={result['kappa']:.2f} | "
                     f"Inv={pricer.inventory:>+5.1f} | "
                     f"WR={pnl.wr:.0%} | PnL={pnl.pnl_pct:>+.3%}")

        await asyncio.sleep(cfg.tick_interval_s)

    pnl.n_rounds+=1
    pnl.log_round(rn)

# =============================================================================
# MAIN — LOOP INFINITO
# =============================================================================

async def main():
    # Credenciais
    secrets = load_secrets("secrets.txt") if LIVE else {
        "PRIVATE_KEY":"0xSIM","API_KEY":"SIM","API_SECRET":"SIM","API_PASSPHRASE":"SIM"
    }

    # Saldo real da carteira
    wallet   = WalletManager(secrets.get("PRIVATE_KEY"))
    bankroll = wallet.get_balance_usdc()

    log.info("="*62)
    log.info("POLYMARKET PURE QUANT MARKET MAKER v3.0")
    log.info("="*62)
    log.info(f"Modo:        {'LIVE' if LIVE else 'PAPER'}")
    log.info(f"Bankroll:    ${bankroll:.4f}")
    log.info(f"Mercado:     XRP 5-min | Loop INFINITO")
    log.info(f"Log:         {os.path.abspath(LOG_FILE)}")
    log.info(f"Para parar:  Ctrl+C")
    log.info("="*62)
    log.info("LOOP INFINITO INICIADO — ronda apos ronda do XRP 5-min")
    log.info("─"*62)

    clob   = PolymarketCLOB(secrets)
    kelly  = KellyEngine()
    pricer = ASStoikovPricer()
    pnl    = PnLTracker(bankroll)
    sim    = PaperSim(0.50)
    rn     = 0

    while True:  # ← LOOP INFINITO
        rn+=1
        log.info(f"[RONDA {rn}] A procurar mercado XRP 5-min...")
        market=None
        while market is None:
            market=MarketFinder.find_xrp_5min()
            if market is None:
                log.warning(f"[RONDA {rn}] Sem mercado. A tentar em 30s...")
                await asyncio.sleep(30)

        # Actualizar saldo em LIVE antes de cada ronda
        if LIVE:
            lb=wallet.get_balance_usdc()
            if lb>0: pnl.bankroll=lb

        try:
            await run_round(rn,market,clob,kelly,pricer,pnl,sim)
        except Exception as e:
            log.error(f"[RONDA {rn}] Erro: {e}. A continuar para proxima ronda...")

        log.info(f"Aguardar {cfg.between_markets_wait_s:.0f}s antes da proxima ronda...")
        await asyncio.sleep(cfg.between_markets_wait_s)

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__=="__main__":
    log.info("BOT INICIADO.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("BOT PARADO (Ctrl+C).")