"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   xrp_true_market_maker_v5.3.4.py                                              ║
║   VERSÃO : 5.3.4 — HFT Ultra (StatArb + OB-Imbalance + VolAdaptive + MicroPrice) ║
║                                                                                  ║
║   DELTA vs v5.3.1:                                                                ║
║     [1] # 6c PolyMakerBridge: LMSR softmax pricing + reset bugfix                ║
║     [2] # 9 SweetSpotPricer: HFTSignalEngine + MicroPrice + OB Imbalance         ║
║     [3] # 16 MonteCarloValidator: perf优化 + HFT tick comment                    ║
║     [4] # 14 run_round: active_orders cleanup anti-leak                          ║
║     [5] Config: microprice_window_ticks, statarb_max_skew_cents + validation    ║
║                                                                                   ║
║   DEPENDÊNCIAS：pip install websockets py-clob-client web3 requests [orjson]   ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations
###############################################################################
# 1 — IMPORTS
###############################################################################
import asyncio, json, logging, logging.handlers, math, os, queue, random
import signal, stat, sys, time, traceback, uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
_HAS_ORJSON: bool = False
try:
    import orjson as _orjson; _HAS_ORJSON = True
    def _json_dumps(o: Any) -> bytes: return _orjson.dumps(o, option=_orjson.OPT_INDENT_2)
    def _json_dumps_compact(o: Any) -> bytes: return _orjson.dumps(o)
    def _json_loads(r: Union[bytes, str]) -> Any: return _orjson.loads(r)
except ImportError:
    def _json_dumps(o: Any) -> bytes: return json.dumps(o, indent=2).encode()
    def _json_dumps_compact(o: Any) -> bytes: return json.dumps(o, separators=(",",":")).encode()
    def _json_loads(r: Union[bytes, str]) -> Any: return json.loads(r)
_HAS_CLOB = False
try:
    from py_clob_client.client import ClobClient as _ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType, ApiCreds
    from py_clob_client.constants import POLYGON; _HAS_CLOB = True
except ImportError: pass
_HAS_WEB3 = False
try: from web3 import Web3; _HAS_WEB3 = True
except ImportError: pass
_HAS_REQUESTS = False
try: import requests as _requests; _HAS_REQUESTS = True
except ImportError: pass
###############################################################################
# 2 — BOT CONFIG + HOT-RELOADING
###############################################################################
def _load_secrets_file(path: str = "secrets.txt") -> Dict[str, str]:
    secrets: Dict[str, str] = {}; p = Path(path)
    if not p.exists(): return secrets
    try:
        fs = os.stat(path)
        if fs.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            logging.getLogger("mm").warning("[SECURITY] %s insecure perms (%s)", path, oct(fs.st_mode)[-3:])
    except OSError: pass
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"): continue
        if "=" in line: k, _, v = line.partition("="); secrets[k.strip()] = v.strip()
    return secrets
_HOT_RELOAD_FIELDS: Dict[str, type] = {
    "volatility_threshold_pct": float, "circuit_breaker_cooldown_sec": float,
    "deadband_cents": float, "skew_exponent": float, "max_inventory_pct": float,
    "band_offsets_cents_0": float, "band_offsets_cents_1": float,
    "band_sizes_usd_0": float, "band_sizes_usd_1": float,
    "inventory_skew_factor": float, "binance_micro_skew_weight": float,
    "tick_interval_s": float, "maker_rebate_bps": float,
    "shadow_latency_ms": float, "shadow_max_slippage_pct": float,
    "lmsr_b": float, "bayesian_prior": float,
    "orderbook_imbalance_weight": float, "vol_regime_sensitivity": float,
    "microprice_alpha": float, "adverse_selection_threshold": float,
}
@dataclass
class BotConfig:
    """v5.3.4 — HFT Ultra (StatArb + OB-Imbalance + VolAdaptive + MicroPrice + BugFix Clean)."""
    dry_run: bool = True
    live_trading: bool = False
    shadow_mode: bool = True
    bankroll_demo: float = 50.0
    # ── Shadow ─────────────────────────────────────────────────────────────
    shadow_latency_ms: float = 100.0
    shadow_max_slippage_pct: float = 0.02
    shadow_fill_prob_base: float = 0.60
    # ── Monte Carlo ────────────────────────────────────────────────────────
    mc_sims: int = 5000
    mc_rounds_per_sim: int = 12           # 12 rounds de 5min = 1 hora
    mc_min_sharpe: float = 0.30           # GO threshold
    mc_max_drawdown_pct: float = 0.30     # NO-GO se maxDD > 30%
    mc_run_at_boot: bool = True
    # ── LMSR / Bayesian (from QR-PM-2026-0041) ────────────────────────────
    lmsr_b: float = 100_000.0             # Liquidity param (higher = tighter spread)
    bayesian_prior: float = 0.50          # Prior belief P(UP)
    bayesian_likelihood_std: float = 0.008
    ev_min_entry: float = 0.005           # |EV| > 0.5% to skew
    # ── Credenciais ─────────────────────────────────────────────────────────
    polymarket_private_key: str = ""
    polymarket_api_key: str = ""
    polymarket_secret: str = ""
    polymarket_passphrase: str = ""
    secrets_path: str = "secrets.txt"
    # ── Ficheiros ──────────────────────────────────────────────────────────
    log_file: str = "mm_v534.log"
    audit_file: str = "mm_v534_audit.jsonl"
    config_json_path: str = "config.json"
    config_reload_interval_s: float = 15.0
    # ── Bandas ─────────────────────────────────────────────────────────────
    band_offsets_cents: Tuple[float, ...] = (0.5, 1.5)
    band_sizes_usd: Tuple[float, ...] = (5.0, 3.0)
    # ── Outras estratégias HFT v5.3.4 (melhorias + limpeza) ────────────────
    orderbook_imbalance_weight: float = 0.45
    vol_regime_sensitivity: float = 1.2
    microprice_alpha: float = 0.65
    adverse_selection_threshold: float = 0.08
    microprice_window_ticks: int = 5                # novo: janela para microprice
    statarb_max_skew_cents: float = 0.8             # clamp explícito
    # ── Defensivas ─────────────────────────────────────────────────────────
    volatility_threshold_pct: float = 0.15
    circuit_breaker_cooldown_sec: float = 15.0
    deadband_cents: float = 0.005
    skew_exponent: float = 2.5
    max_inventory_pct: float = 0.85
    # ── Inventory ──────────────────────────────────────────────────────────
    max_inventory_frac: float = 0.30
    inventory_skew_factor: float = 0.008
    emergency_inventory_frac: float = 0.50
    inventory_sync_interval_s: float = 30.0
    # ── Binance ──────────────────────────────────────────────────────────
    binance_enabled: bool = True
    binance_ws_uri: str = "wss://stream.binance.com:9443/ws/xrpusdt@ticker"
    binance_btc_ws_uri: str = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    binance_reconnect_base_s: float = 1.2
    binance_reconnect_max_s: float = 30.0
    binance_ping_interval_s: float = 20.0
    binance_micro_skew_weight: float = 0.15
    binance_drift_window: int = 30
    binance_drift_threshold: float = 0.003
    vol_window_seconds: float = 5.0
    # ── Timing ─────────────────────────────────────────────────────────────
    tick_interval_s: float = 0.1  # HFT tick
    market_duration_s: float = 300.0
    between_markets_s: float = 10.0
    log_every_n_ticks: int = 5
    # ── Fees ───────────────────────────────────────────────────────────────
    maker_rebate_bps: float = 20.0
    min_spread_cents: float = 1.0
    # ── WS ─────────────────────────────────────────────────────────────────
    heartbeat_interval_s: float = 5.0
    heartbeat_max_errors: int = 10
    # ── Endpoints ──────────────────────────────────────────────────────────
    clob_rest_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    polygon_rpc: str = "https://polygon-rpc.com"
    usdc_contract: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    max_api_retries: int = 4
    base_backoff_s: float = 1.1
    max_backoff_s: float = 35.0
    def __post_init__(self): self._validate()
    def _validate(self):
        if self.live_trading and not self.polymarket_private_key: raise ValueError("key required")
        if self.live_trading and self.shadow_mode: raise ValueError("shadow+live conflict")
        if len(self.band_offsets_cents) != len(self.band_sizes_usd): raise ValueError("bands mismatch")
        if self.shadow_latency_ms < 0: raise ValueError("latency >=0")
        if not (0.0 < self.shadow_max_slippage_pct <= 0.10): raise ValueError("slip in (0,0.10]")
        if self.tick_interval_s < 0.05: raise ValueError("tick>=0.05")  # HFT OK
        if self.skew_exponent < 1.0: raise ValueError("skew>=1.0")
        if self.lmsr_b <= 0: raise ValueError("lmsr_b>0")
        if self.mc_sims < 100: raise ValueError("mc_sims>=100")
    @classmethod
    def from_env_and_secrets(cls, secrets_path="secrets.txt") -> "BotConfig":
        sf = _load_secrets_file(secrets_path)
        def _g(k,d=" ",*a):
            for x in (k,*a):
                if x in sf: return sf[x]
            for x in (k,*a):
                v=os.environ.get(x," ")
                if v: return v
            return d
        c=cls.__new__(cls)
        for f in cls.__dataclass_fields__.values():
            try: setattr(c,f.name,f.default)
            except: pass
        c.secrets_path=secrets_path
        c.polymarket_private_key=_g("PRIVATE_KEY"," ","POLYMARKET_PRIVATE_KEY")
        c.polymarket_api_key=_g("API_KEY"," ","POLYMARKET_API_KEY")
        c.polymarket_secret=_g("API_SECRET"," ","POLYMARKET_SECRET")
        c.polymarket_passphrase=_g("API_PASSPHRASE"," ","POLYMARKET_PASSPHRASE")
        if _g("DRY_RUN","1").lower() in ("0","false","no"): c.dry_run=False
        if _g("LIVE_TRADING","0").lower() in ("1","true","yes"): c.live_trading=True
        if _g("SHADOW_MODE","1").lower() in ("0","false","no"): c.shadow_mode=False
        c._validate(); return c
async def config_hot_reload_loop(cfg: BotConfig):
    path=Path(cfg.config_json_path); mt=0.0
    while True:
        try:
            await asyncio.sleep(cfg.config_reload_interval_s)
            if not path.exists(): continue
            nm=path.stat().st_mtime
            if nm <=mt: continue
            mt=nm; data=json.loads(path.read_text("utf-8")); ch=[]
            for k,t in _HOT_RELOAD_FIELDS.items():
                if k not in data: continue
                nv=t(data[k]); ov=getattr(cfg,k,None)
                if ov is not None and abs(float(nv)-float(ov)) >1e-9:
                    setattr(cfg,k,nv); ch.append(f"{k}:{ov}→{nv}")
            for pfx,attr in [("band_offsets_cents_","band_offsets_cents"),("band_sizes_usd_","band_sizes_usd")]:
                if any(k.startswith(pfx) for k in data):
                    lst=list(getattr(cfg,attr))
                    for i in range(len(lst)):
                        kk=f"{pfx}{i}"
                        if kk in data: lst[i]=float(data[kk])
                    setattr(cfg,attr,tuple(lst))
            if ch: _get_log().info("[CONFIG_UPDATE] %d: %s",len(ch)," | ".join(ch))
        except asyncio.CancelledError: return
        except Exception: pass
###############################################################################
# 3 — LOGGING
###############################################################################
_log_listener: Optional[logging.handlers.QueueListener] = None
log: Optional[logging.Logger] = None
def init_logging(lf="mm_v534.log") -> logging.Logger:
    global log, _log_listener
    q: queue.Queue=queue.Queue(-1)
    fmt=logging.Formatter("%(asctime)s.%(msecs)03d | %(levelname)-5s | %(message)s","%d/%m/%y %H:%M:%S")
    ch=logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); ch.setLevel(logging.INFO)
    fh=logging.handlers.RotatingFileHandler(lf,maxBytes=10*1024*1024,backupCount=5,encoding="utf-8")
    fh.setFormatter(fmt); fh.setLevel(logging.DEBUG)
    _log_listener=logging.handlers.QueueListener(q,fh,ch,respect_handler_level=True); _log_listener.start()
    l=logging.getLogger("mm_v534"); l.setLevel(logging.DEBUG); l.handlers.clear()
    l.addHandler(logging.handlers.QueueHandler(q)); l.propagate=False
    for n in ("websockets","websockets.client","urllib3","web3"): logging.getLogger(n).setLevel(logging.WARNING)
    log=l; return l
def _get_log(): return log or logging.getLogger("mm_v534")
def _ts(): return datetime.now().strftime("%d/%m/%y %H:%M:%S.%f")[:-3]
def _uptime(s):
    e=int(time.time()-s); h,r=divmod(e,3600); m,s2=divmod(r,60); return f"{h:02d}h:{m:02d}m:{s2:02d}s"
def _fc(p): return f"{p*100:.1f}c"
def _fmt_dollar(v): return f"${v:+.6f}" if v >=0 else f"-${abs(v):.6f}"
def _fmt_pct(v): return f"{v:+.2f}%"
def _log_sep(): _get_log().info("-"*78)
def _log_sep2(): _get_log().info("="*78)
###############################################################################
# 4 — AUDIT + FEE CALC
###############################################################################
class AuditLogger:
    def __init__(self, af="mm_v534_audit.jsonl"):
        self._fh=None
        try: self._fh=logging.FileHandler(af,encoding="utf-8")
        except: pass
    def _emit(self, r):
        r["ts"]=datetime.now(timezone.utc).isoformat()
        try:
            line=_json_dumps_compact(r).decode()+"\n"
            if self._fh: self._fh.stream.write(line); self._fh.stream.flush()
        except: pass
    def log_order(self,a,s,t,p,sz,oid=""): self._emit({"event":"ORDER","action":a,"side":s,"token":t[:20],"price":p,"size":sz,"oid":oid[:16]})
    def log_fill(self,s,t,p,sz,pnl,reb,shadow=False): self._emit({"event":"SHADOW_FILL" if shadow else "FILL","side":s,"token":t[:20],"price":p,"size":sz,"pnl":pnl,"rebate":reb})
    def log_event(self,c,m): self._emit({"event":"BOT_EVENT","cat":c,"msg":m})
    def log_error(self,c,m): self._emit({"event":"ERROR","cat":c,"msg":m}); _get_log().error("[AUDIT] %s|%s",c,m)
    def log_circuit_breaker(self,tr,v,co): self._emit({"event":"CB","trigger":tr,"vol":v,"cool":co})
    def log_shutdown(self,bk,pnl,up): self._emit({"event":"SHUTDOWN","bk":bk,"pnl":pnl,"up":up})
def polymarket_taker_fee(shares, price):
    if price<=0 or price>=1: return 0.0
    return shares*price*0.25*(price*(1-price))**2
def polymarket_maker_rebate(shares, price, bps=20.0):
    return shares*price*bps/10000.0
###############################################################################
# 5 — WALLET
###############################################################################
class WalletManager:
    def __init__(self, cfg):
        self._cfg=cfg; self.address=None; self._w3=None
        if cfg.live_trading and _HAS_WEB3 and cfg.polymarket_private_key:
            try: self._w3=Web3(Web3.HTTPProvider(cfg.polygon_rpc)); self.address=self._w3.eth.account.from_key(cfg.polymarket_private_key).address
            except Exception as e: _get_log().error("[WALLET] %s",e)
    def get_balance_usdc(self):
        if not self._cfg.live_trading: return self._cfg.bankroll_demo
        if not self._w3 or not self.address: return self._cfg.bankroll_demo
        try:
            abi=[{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
            return self._w3.eth.contract(address=Web3.to_checksum_address(self._cfg.usdc_contract),abi=abi).functions.balanceOf(self.address).call()/1e6
        except: return self._cfg.bankroll_demo
    def get_token_balance(self,tc):
        if not self._w3 or not self.address: return 0.0
        try:
            abi=[{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
            return float(self._w3.eth.contract(address=Web3.to_checksum_address(tc),abi=abi).functions.balanceOf(self.address).call())/1e6
        except: return 0.0
###############################################################################
# 6 — POLYMARKET CLOB (shadow-aware)
###############################################################################
class PolymarketCLOB:
    def __init__(self, cfg):
        self._cfg=cfg; self._client=None; self._active_orders: Dict[str,dict]={}; self.api_calls=0
        if (cfg.shadow_mode or cfg.live_trading) and _HAS_CLOB and cfg.polymarket_api_key:
            try:
                creds=ApiCreds(api_key=cfg.polymarket_api_key,api_secret=cfg.polymarket_secret,api_passphrase=cfg.polymarket_passphrase)
                self._client=_ClobClient(host=cfg.clob_rest_url,key=cfg.polymarket_private_key or "0x"+"0"*39+"1",chain_id=POLYGON,creds=creds)
                _get_log().info("[CLOB] Init: %s","SHADOW(RO)" if cfg.shadow_mode else "LIVE")
            except Exception as e: _get_log().error("[CLOB] Init fail: %s",e)
        else: _get_log().info("[CLOB] PAPER mode")
    def get_order_book(self, token_id):
        if self._client:
            try: self.api_calls+=1; return self._client.get_order_book(token_id)
            except: pass
        if _HAS_REQUESTS:
            try:
                self.api_calls+=1; r=_requests.get(f"{self._cfg.clob_rest_url}/book",params={"token_id":token_id},timeout=8)
                if r.status_code==200: return r.json()
            except: pass
        return None
    async def post_limit_order(self, token_id, side, price, size_usd, band_idx=0, audit=None):
        price=round(max(0.01,min(0.99,price)),2)
        shares=round(size_usd/price,2) if price >0.01 else 0.0
        if shares <0.01: return None
        if self._cfg.live_trading and self._client and not self._cfg.shadow_mode:
            try:
                self.api_calls+=1
                args=OrderArgs(token_id=token_id,price=round(price,2),size=shares,side=side,order_type=OrderType.GTC)
                resp=self._client.create_and_post_order(args)
                oid=resp.get("orderID") or resp.get("order_id","unk")
                self._active_orders[oid]={"order_id":oid,"side":side,"price":price,"size":shares,"size_usd":size_usd,"token_id":token_id,"band":band_idx,"ts":time.time()}
                if audit: audit.log_order("POST",side,token_id,price,shares,oid)
                return oid
            except Exception as e:
                s=str(e).lower()
                if "crossed" in s or "taker" in s: _get_log().warning("[CLOB] Rejected cross %s@%s",side,_fc(price))
                else: _get_log().error("[CLOB] Post err: %s",e)
                return None
        oid=f"shd_{side}_{token_id[:6]}_{uuid.uuid4().hex[:8]}"
        self._active_orders[oid]={"order_id":oid,"side":side,"price":price,"size":shares,"size_usd":size_usd,"token_id":token_id,"band":band_idx,"ts":time.time()}
        return oid
    async def cancel_order(self, oid, audit=None):
        self._active_orders.pop(oid,None)
        if self._cfg.live_trading and self._client and not self._cfg.shadow_mode:
            try: self.api_calls+=1; self._client.cancel(oid); return True
            except: return False
        return True
    async def cancel_all(self, audit=None):
        cnt=len(self._active_orders)
        if self._cfg.live_trading and self._client and not self._cfg.shadow_mode:
            try:
                ids=list(self._active_orders.keys())
                if ids: self.api_calls+=1; self._client.cancel_orders(ids)
            except: pass
        if audit: audit.log_event("CLOB",f"cancel_all:{cnt}")
        self._active_orders.clear(); return cnt
    async def get_fills(self, token_id):
        if not self._cfg.live_trading or not self._client or self._cfg.shadow_mode: return []
        try: self.api_calls+=1; t=self._client.get_trades({"maker":True,"token_id":token_id}); return t.get("data",[]) if isinstance(t,dict) else []
        except: return []
###############################################################################
# 6b — SHADOW FILL ENGINE
###############################################################################
@dataclass
class ShadowFill:
    order_id:str; side:str; book:str; token_id:str
    order_price:float; fill_price:float; shares:float; rebate:float
    latency_ms:float; slippage_pct:float; ts:float
class ShadowFillEngine:
    """Match shadow_orders vs livro L2 real: latência 100ms, slippage ≤2%, partial fills."""
    def __init__(self, cfg): self._cfg=cfg; self.total_shadow_fills=0; self.total_rejected=0
    def try_match(self, clob, ob_up, ob_down, tu, td):
        cfg=self._cfg; fills=[]; now=time.time(); lat_s=cfg.shadow_latency_ms/1000.0
        for oid,info in list(clob._active_orders.items()):
            age=now-info["ts"]
            if age <lat_s: continue
            side=info["side"]; price=info["price"]; shares=info["size"]; tid=info["token_id"]
            if tid==tu: book="UP"; snap=ob_up
            elif tid==td: book="DOWN"; snap=ob_down
            else: continue
            if snap.stale: continue
            can=False; liq=0.0
            if side=="BUY":
                if snap.best_ask >0 and snap.best_ask <=price:
                    can=True; liq=sum(s for p,s in snap.ask_levels if p <=price)
            else:
                if snap.best_bid >0 and snap.best_bid >=price:
                    can=True; liq=sum(s for p,s in snap.bid_levels if p >=price)
            if not can: continue
            if random.random() >cfg.shadow_fill_prob_base: continue
            fsh=min(shares,max(0.01,liq*0.3))
            slip=random.uniform(0,cfg.shadow_max_slippage_pct)
            if side=="BUY": fp=min(price*(1+slip),price*(1+cfg.shadow_max_slippage_pct))
            else: fp=max(price*(1-slip),price*(1-cfg.shadow_max_slippage_pct))
            fp=round(max(0.01,min(0.99,fp)),4); aslip=abs(fp-price)/(price+1e-9)
            reb=polymarket_maker_rebate(fsh,fp,cfg.maker_rebate_bps)
            fills.append(ShadowFill(oid,side,book,tid,price,fp,fsh,reb,age*1000,aslip,now))
            self.total_shadow_fills+=1
            if fsh >=shares*0.99: clob._active_orders.pop(oid,None)
            else: info["size"]-=fsh; info["size_usd"]=info["size"]*info["price"]
        return fills
###############################################################################
# 6c — POLYMAKER BRIDGE (LMSR + Bayesian Signal)
    ###############################################################################
class LMSRPricer:
    """Logarithmic Market Scoring Rule pricer (QR-PM-2026-0041 §1-#2).
    Cost: C(q) = b · ln(Σ e^(qi/b))
    Price (softmax): pi(q) = e^(qi/b) / Σ e^(qj/b)
        For binary (n=2): p_up = e^(q_up/b) / (e^(q_up/b) + e^(q_down/b))
    Detecta ineficiência quando |p_lmsr - p_market| > threshold.
        """
    def __init__(self, b: float = 100_000.0):
        self.b = b
        self._q_up: float = 0.0
        self._q_down: float = 0.0
    def update_quantities(self, q_up: float, q_down: float) -> None:
        self._q_up = q_up
        self._q_down = q_down
    def cost(self) -> float:
        """C(q) = b · ln(e^(q_up/b) + e^(q_down/b))"""
        b = self.b
        mx = max(self._q_up / b, self._q_down / b)
        return b * (mx + math.log(math.exp(self._q_up / b - mx) + math.exp(self._q_down / b - mx)))
    def price_up(self) -> float:
        """Softmax: p_up = e^(q_up/b) / (e^(q_up/b) + e^(q_down/b))"""
        b = self.b
        diff = (self._q_up - self._q_down) / b
        diff = max(-20.0, min(20.0, diff))
        return 1.0 / (1.0 + math.exp(-diff))
    def price_down(self) -> float:
        return 1.0 - self.price_up()
    def max_loss(self) -> float:
        """L_max = b · ln(2) ≈ b × 0.6931"""
        return self.b * math.log(2.0)
    def detect_inefficiency(self, market_price_up: float) -> float:
        """Retorna EV = p̂_lmsr - p_market. Positivo = UP underpriced."""
        return self.price_up() - market_price_up
class BayesianSignal:
    """Sequential Bayesian updating em log-space (QR-PM-2026-0041 #2).  
    log P(H|D) = log P(H) + Σ log P(Dk|H) - log Z
    Actualiza belief P(UP) a cada tick de preço Binance.
    Usado para ajustar skew do pricer: se P(UP) >0.55, quote mais agressivo no UP.
    """
    def __init__(self, prior: float = 0.50, likelihood_std: float = 0.008):
        self._log_odds: float = math.log(prior / (1.0 - prior + 1e-15))
        self._std = likelihood_std
        self._updates: int = 0
    @property
    def p_up(self) -> float:
        """Posterior P(UP) via sigmoid do log-odds."""
        lo = max(-15.0, min(15.0, self._log_odds))
        return 1.0 / (1.0 + math.exp(-lo))
    @property
    def p_down(self) -> float:
        return 1.0 - self.p_up
    def update(self, xrp_return: float) -> None:
        """Actualiza com retorno observado. Positivo = evidence for UP."""
        log_lr = xrp_return / (self._std ** 2 + 1e-15)
        log_lr = max(-5.0, min(5.0, log_lr))
        self._log_odds += log_lr
        self._log_odds = max(-10.0, min(10.0, self._log_odds))
        self._updates += 1
    def reset(self, prior: float = 0.50) -> None:
        self._log_odds = math.log(prior / (1.0 - prior + 1e-15))
        self._updates = 0
    def ev_signal(self, market_price_up: float) -> float:
        """EV = p̂ − p  (QR-PM-2026-0041 eq.4). Entry signal."""
        return self.p_up - market_price_up
class PolyMakerBridge:
    """Ponte entre o pricer e a lógica poly-maker.
    Combina LMSR fair value + Bayesian signal para gerar um skew_adjustment
    que é aplicado pelo SweetSpotPricer. Não substitui o pricer — augmenta-o.
    O skew é clampado a ±1c para nunca dominar o pricing base.
    """
    def __init__(self, cfg: BotConfig):
        self.lmsr = LMSRPricer(cfg.lmsr_b)
        self.bayesian = BayesianSignal(cfg.bayesian_prior, cfg.bayesian_likelihood_std)
        self._cfg = cfg
        self._last_xrp_price: Optional[float] = None
    def on_xrp_tick(self, price: float) -> None:
        """Chamado a cada tick Binance. Actualiza Bayesian belief."""
        if self._last_xrp_price is not None and self._last_xrp_price > 0:
            ret = (price - self._last_xrp_price) / self._last_xrp_price
            self.bayesian.update(ret)
        self._last_xrp_price = price
    def compute_skew_adjustment(self, market_mid_up: float) -> float:
        """Calcula ajuste de skew baseado em LMSR + Bayesian EV.
        Returns: skew em unidades de preço (±). Positivo = favorecer UP bids.
        Clampado a ±0.01 (1c) para não dominar o pricing exponencial.
        """
        ev_bayes = self.bayesian.ev_signal(market_mid_up)
        ev_lmsr = self.lmsr.detect_inefficiency(market_mid_up)
        combined_ev = 0.6 * ev_bayes + 0.4 * ev_lmsr
        if abs(combined_ev) < self._cfg.ev_min_entry:
            return 0.0
        skew = combined_ev * 0.10
        return max(-0.01, min(0.01, skew))
    # BUGFIX v5.3.4: limpar reset (evita stale skew)
    def reset(self) -> None:
        self.bayesian.reset(self._cfg.bayesian_prior)
        self._last_xrp_price = None
        self.lmsr.update_quantities(0.0, 0.0)  # força reset LMSR
###############################################################################
#7 — MARKET FINDER (deterministic slug)
###############################################################################
_WINDOW_S=300; _MIN_REM=25
class MarketFinder:
    _consecutive_failures=0; _FALLBACK_AFTER=3
    @staticmethod
    def _calc_window():
        now=time.time(); s=int(now//_WINDOW_S)*_WINDOW_S; return s,s+_WINDOW_S,s+_WINDOW_S-now
    @staticmethod
    def find_xrp_5min(cfg):
        use=cfg.live_trading or cfg.shadow_mode
        if not use or not _HAS_REQUESTS: return MarketFinder._sim(cfg)
        if cfg.shadow_mode and MarketFinder._consecutive_failures>=MarketFinder._FALLBACK_AFTER:
            _get_log().info("[MARKET] Shadow fallback→sim"); MarketFinder._consecutive_failures=0; return MarketFinder._sim(cfg)
        s,e,tr=MarketFinder._calc_window()
        if tr<_MIN_REM: s+=_WINDOW_S; e+=_WINDOW_S; tr+=_WINDOW_S
        slug=f"xrp-updown-5m-{s}"
        r=MarketFinder._fetch(slug,e,cfg)
        if r: MarketFinder._consecutive_failures=0; return r
        ps=s-_WINDOW_S; pe=s
        if pe-time.time()>_MIN_REM:
            r2=MarketFinder._fetch(f"xrp-updown-5m-{ps}",pe,cfg)
            if r2: MarketFinder._consecutive_failures=0; return r2
        r3=MarketFinder._search(cfg)
        if r3: MarketFinder._consecutive_failures=0; return r3
        MarketFinder._consecutive_failures+=1; return None
    @staticmethod
    def _fetch(slug,ets,cfg):
        try:
            r=_requests.get(f"{cfg.gamma_api_url}/events",params={"slug":slug},timeout=8)
            if r.status_code!=200: return None
            d=r.json(); evs=d if isinstance(d,list) else [d] if isinstance(d,dict) and d else []
            for ev in evs:
                for m in (ev.get("markets") or []):
                    p=MarketFinder._parse(m,ets)
                    if p: _get_log().info("[MARKET] FOUND slug=%s | %s | T-rem:%.0fs",slug,p["question"][:40],p["_end_ts"]-time.time()); return p
                p2=MarketFinder._parse(ev,ets)
                if p2: return p2
        except: pass
        return None
    @staticmethod
    def _search(cfg):
        try:
            r=_requests.get(f"{cfg.gamma_api_url}/events",params={"active":"true","closed":"false","limit":30},timeout=10)
            if r.status_code!=200: return None
            evs=r.json()
            if not isinstance(evs,list): evs=[evs] if evs else []
            for ev in evs:
                if "xrp" not in str(ev).lower(): continue
                for m in (ev.get("markets") or []):
                    import datetime as _dt
                    es=(m.get("endDateIso") or m.get("end_date_iso") or " ")
                    try: et=_dt.datetime.fromisoformat(es.replace("Z","+00:00")).timestamp()
                    except: et=time.time()+_WINDOW_S
                    p=MarketFinder._parse(m,et)
                    if p: return p
        except: pass
        return None
    @staticmethod
    def _parse(m,fallback_end):
        toks=m.get("clobTokenIds") or m.get("tokenIds") or m.get("clob_token_ids") or []
        if not isinstance(toks,list): toks=[]
        if len(toks) <2: return None
        if m.get("closed") in (True,"true"): return None
        now=time.time()
        es=m.get("endDateIso") or m.get("end_date_iso") or m.get("endDate") or " "
        try:
            import datetime as _dt
            et=_dt.datetime.fromisoformat(es.replace("Z","+00:00")).timestamp() if es else fallback_end
        except: et=fallback_end
        if et-now <_MIN_REM: return None
        return {"conditionId":m.get("conditionId"," "),"question":m.get("question") or m.get("title","XRP 5m"),
                "tokenIds":[str(t) for t in toks],"clobTokenIds":[str(t) for t in toks],"_end_ts":et}
    @staticmethod
    def _sim(cfg):
        s,e,_=MarketFinder._calc_window()
        return {"conditionId":f"0xSIM_{s}","question":f"[SIM] XRP 5m ({s})","tokenIds":["SIM_UP","SIM_DN"],"_end_ts":e,"_simulated":True}
    @staticmethod
    def get_token_ids(m):
        toks=m.get("clobTokenIds") or m.get("tokenIds") or []
        if len(toks) >=2: return str(toks[0]),str(toks[1])
        c=m.get("conditionId","X"); return f"{c}_UP",f"{c}_DN"
###############################################################################
#8 — ORDERBOOK
###############################################################################
@dataclass
class OrderBookSnapshot:
    best_bid:float=0; best_ask:float=0; midpoint:float=0.50; spread:float=0
    bid_depth:float=0; ask_depth:float=0
    bid_levels:List[Tuple[float,float]]=field(default_factory=list)
    ask_levels:List[Tuple[float,float]]=field(default_factory=list)
    last_update_ts:float=0; stale:bool=True
    def update(self, bids, asks):
        self.bid_levels=[(float(b.get("price",0)),float(b.get("size",0))) for b in bids[:10] if float(b.get("price",0)) >0]
        self.ask_levels=[(float(a.get("price",0)),float(a.get("size",0))) for a in asks[:10] if float(a.get("price",0)) >0]
        if self.bid_levels: self.best_bid=self.bid_levels[0][0]; self.bid_depth=sum(s for _,s in self.bid_levels[:5])
        else: self.best_bid=0; self.bid_depth=0
        if self.ask_levels: self.best_ask=self.ask_levels[0][0]; self.ask_depth=sum(s for _,s in self.ask_levels[:5])
        else: self.best_ask=0; self.ask_depth=0
        if self.best_bid >0 and self.best_ask >0: self.midpoint=(self.best_bid+self.best_ask)/2; self.spread=self.best_ask-self.best_bid; self.stale=False
        elif self.best_bid >0: self.midpoint=self.best_bid; self.stale=True
        elif self.best_ask >0: self.midpoint=self.best_ask; self.stale=True
        else: self.stale=True
        self.last_update_ts=time.time()
    def is_stale(self, th=10.0): return self.stale or (time.time()-self.last_update_ts) >th
@dataclass
class DualOrderBook:
    up:OrderBookSnapshot=field(default_factory=OrderBookSnapshot)
    down:OrderBookSnapshot=field(default_factory=OrderBookSnapshot)
    def update_from_clob(self, clob, tu, td):
        bu=clob.get_order_book(tu)
        if bu: self.up.update(bu.get("bids",[]),bu.get("asks",[]))
        bd=clob.get_order_book(td)
        if bd: self.down.update(bd.get("bids",[]),bd.get("asks",[]))
    def simulate(self):
        um=max(.1,min(.9,.5+random.gauss(0,.005))); sp=max(.01,random.gauss(.02,.005))
        self.up.best_bid=round(um-sp/2,2); self.up.best_ask=round(um+sp/2,2); self.up.midpoint=um; self.up.spread=sp
        self.up.bid_depth=random.uniform(50,500); self.up.ask_depth=random.uniform(50,500)
        self.up.bid_levels=[(round(um-.01*i-sp/2,2),random.uniform(20,100)) for i in range(5)]
        self.up.ask_levels=[(round(um+.01*i+sp/2,2),random.uniform(20,100)) for i in range(5)]
        self.up.stale=False; self.up.last_update_ts=time.time()
        dm=max(.1,min(.9,1-um+random.gauss(0,.003))); sp2=max(.01,random.gauss(.02,.005))
        self.down.best_bid=round(dm-sp2/2,2); self.down.best_ask=round(dm+sp2/2,2); self.down.midpoint=dm; self.down.spread=sp2
        self.down.bid_depth=random.uniform(50,500); self.down.ask_depth=random.uniform(50,500)
        self.down.bid_levels=[(round(dm-.01*i-sp2/2,2),random.uniform(20,100)) for i in range(5)]
        self.down.ask_levels=[(round(dm+.01*i+sp2/2,2),random.uniform(20,500)) for i in range(5)]
        self.down.stale=False; self.down.last_update_ts=time.time()
###############################################################################
#9 — SWEET SPOT PRICER (HFT Ultra v5.3.4)
###############################################################################
@dataclass
class BandQuote:
    side:str; book:str; price:float; size_usd:float; band_idx:int
class HFTSignalEngine:
    """HFT signals: Orderbook Imbalance, Vol Adaptive, StatArb skew."""
    def __init__(self, cfg: BotConfig):
        self._cfg = cfg
        self._last_imbalance: Dict[str, float] = {"UP": 0.0, "DOWN": 0.0}
        self._vol_regime: float = 0.0
        self._bin_price: float = 0.0
    def compute_ob_imbalance(self, snap: OrderBookSnapshot) -> float:
        if snap.bid_depth + snap.ask_depth < 1e-9: return 0.0
        return (snap.bid_depth - snap.ask_depth) / (snap.bid_depth + snap.ask_depth)
    def compute_statarb_skew(self, bin_price: float, poly_mid: float) -> float:
        if bin_price <= 0 or poly_mid <= 0: return 0.0
        diff = (bin_price - poly_mid) / poly_mid
        skew = diff * self._cfg.binance_micro_skew_weight
        skew = max(-self._cfg.statarb_max_skew_cents/100, min(self._cfg.statarb_max_skew_cents/100, skew))
        return skew
    def compute_vol_adaptive_size(self, base_size: float, vol: float) -> float:
        if vol <= 0: return base_size
        if vol < 0.08: return base_size * 1.5  # low vol = bigger size
        elif vol < 0.15: return base_size
        else: return base_size * 0.6  # high vol = smaller size
@dataclass
class SweetSpotPricer:
    __slots__=('_cfg','_binance_drift','_polymaker_skew','_hft','_microprice','_vol_regime','_bin_price')
    def __init__(self, cfg):
        self._cfg=cfg; self._binance_drift=0.0; self._polymaker_skew=0.0
        self._hft = HFTSignalEngine(cfg)
        self._microprice: deque = deque(maxlen=cfg.microprice_window_ticks)
        self._vol_regime: float = 0.0
        self._bin_price: float = 0.0
    def set_binance_drift(self, d): self._binance_drift=max(-.02,min(.02,d))
    def set_polymaker_skew(self, s): self._polymaker_skew=max(-.01,min(.01,s))
    def set_hft_signals(self, imb_up: float, imb_down: float, vol: float, bin_price: float):
        self._hft._last_imbalance["UP"] = imb_up
        self._hft._last_imbalance["DOWN"] = imb_down
        self._vol_regime = vol
        self._bin_price = bin_price
    def _update_microprice(self, snap: OrderBookSnapshot):
        if snap.best_bid <= 0 or snap.best_ask <= 0: return
        imb = (snap.bid_depth - snap.ask_depth) / (snap.bid_depth + snap.ask_depth + 1e-9)
        self._microprice.append(snap.best_bid * (1 + imb) / 2)
    def compute_quotes(self, ob, inv_up, inv_down, bk):
        q=[]; mi=bk*self._cfg.max_inventory_pct; em=bk*self._cfg.emergency_inventory_frac
        # v5.3.4: microprice + limpeza
        self._update_microprice(ob.up)
        self._update_microprice(ob.down)
        mp_up = sum(self._microprice) / len(self._microprice) if self._microprice else ob.up.midpoint
        imb_u = self._hft.compute_ob_imbalance(ob.up)
        imb_d = self._hft.compute_ob_imbalance(ob.down)
        vol_adj = _vol_oracle._mv(_vol_oracle.xrp_prices, _vol_oracle.vol_window_seconds) if _vol_oracle.xrp_prices else 0.0
        if hasattr(self, '_bin_price') and self._bin_price:
            stat_skew = self._hft.compute_statarb_skew(self._bin_price, ob.up.midpoint)
            self.set_polymaker_skew(stat_skew)
        if not ob.up.is_stale(): q.extend(self._book(ob.up,"UP",inv_up,mi,em, imb_u, vol_adj, mp_up))
        if not ob.down.is_stale(): q.extend(self._book(ob.down,"DOWN",inv_down,mi,em, imb_d, vol_adj, mp_up))
        return q
    def _book(self, snap, book, inv, mi, em, imb: float = 0.0, vol: float = 0.0, microprice: float = 0.0):
        cfg=self._cfg; q=[]; mid=snap.midpoint; sp=max(snap.spread,cfg.min_spread_cents/100)
        ir=min(1.0,abs(inv)/(mi+1e-9)); sm=sp*(ir**cfg.skew_exponent); sk=sm*(1 if inv>0 else -1)
        mc=0.0
        if cfg.binance_enabled and abs(self._binance_drift) >cfg.binance_drift_threshold:
            mc=self._binance_drift*cfg.binance_micro_skew_weight*(1 if book=="UP" else -1)
        pm_sk = self._polymaker_skew * (1 if book=="UP" else -1)
        am=mid+mc+pm_sk
        # HFT v5.3.4: microprice blend (mantém limpo)
        if microprice > 0:
            am = cfg.microprice_alpha * microprice + (1 - cfg.microprice_alpha) * am
        # HFT: Orderbook Imbalance skew
        if abs(imb) > cfg.adverse_selection_threshold:
            am += cfg.orderbook_imbalance_weight * imb * 0.012
        # HFT: Vol Adaptive (low-vol = tighter + bigger size)
        if vol < 0.08:
            am = mid + (am - mid) * 0.75
        # BUGFIX: clamp explícito para evitar preços inválidos
        am = max(0.01, min(0.99, am))
        sb=inv>=mi; ag=inv>=em; sa=inv<=-em
        bo=self._sweet(snap.bid_levels,mid); ao=self._sweet(snap.ask_levels,mid)
        for i,sz in enumerate(cfg.band_sizes_usd):
            sz_adj = self._hft.compute_vol_adaptive_size(sz, vol) if vol > 0 else sz
            bf=bo[i] if i<len(bo) else cfg.band_offsets_cents[-1]/100
            af=ao[i] if i<len(ao) else cfg.band_offsets_cents[-1]/100
            if not sb:
                bp=round(max(.01,min(.99,am-bf-sk)),2)
                if bp <round(am,2): q.append(BandQuote("BUY",book,bp,sz_adj,i))
            if not sa:
                ap=am+af-sk
                if ag: ap=am+max(.01,af*.5)-sk
                ap=round(max(.01,min(.99,ap)),2)
                if ap >round(am,2): q.append(BandQuote("SELL",book,ap,sz_adj,i))
        return q
    def _sweet(self, levels, mid):
        cfg=self._cfg
        if not levels or len(levels) <2: return [c/100 for c in cfg.band_offsets_cents]
        off=[]; cum=0.0
        for p,s in levels:
            d=abs(p-mid); cum+=s
            if cum >25 and len(off) <len(cfg.band_offsets_cents): off.append(max(.01,d-.01)); cum=0
        while len(off) <len(cfg.band_offsets_cents): off.append(cfg.band_offsets_cents[len(off)]/100)
        return off
###############################################################################
#10 — INVENTORY
###############################################################################
class InventoryManager:
    __slots__=('up_shares','up_avg_price','down_shares','down_avg_price','_fc_up','_fc_dn','_last_sync_ts')
    def __init__(self):
        self.up_shares=0.0; self.up_avg_price=0.0; self.down_shares=0.0; self.down_avg_price=0.0
        self._fc_up=0; self._fc_dn=0; self._last_sync_ts=0.0
    def record_buy(self,b,p,s):
        if b=="UP": tc=self.up_avg_price*self.up_shares+p*s; self.up_shares+=s; self.up_avg_price=tc/self.up_shares if self.up_shares >0 else p; self._fc_up+=1
        else: tc=self.down_avg_price*self.down_shares+p*s; self.down_shares+=s; self.down_avg_price=tc/self.down_shares if self.down_shares >0 else p; self._fc_dn+=1
    def record_sell(self,b,s):
        if b=="UP": self.up_shares=max(0,self.up_shares-s); (setattr(self,'up_avg_price',0) if self.up_shares <=.001 else None); self.up_shares=max(0,self.up_shares); self._fc_up+=1
        else: self.down_shares=max(0,self.down_shares-s); (setattr(self,'down_avg_price',0) if self.down_shares <=.001 else None); self.down_shares=max(0,self.down_shares); self._fc_dn+=1
    @property
    def up_value_usd(self): return self.up_shares*self.up_avg_price
    @property
    def down_value_usd(self): return self.down_shares*self.down_avg_price
    @property
    def total_fills(self): return self._fc_up+self._fc_dn
    def mark_to_market(self,um,dm): return self.up_shares*um+self.down_shares*dm
    def unrealized_pnl(self,um,dm): return self.mark_to_market(um,dm)-(self.up_value_usd+self.down_value_usd)
    def sync_from_api(self,w,tu,td,cfg):
        now=time.time()
        if now-self._last_sync_ts <cfg.inventory_sync_interval_s: return False
        self._last_sync_ts=now
        if not cfg.live_trading or not _HAS_WEB3: return False
        c=False
        try:
            ru=w.get_token_balance(tu)
            if abs(ru-self.up_shares) >.01: self.up_shares=ru; c=True
            rd=w.get_token_balance(td)
            if abs(rd-self.down_shares) >.01: self.down_shares=rd; c=True
        except: pass
        return c
    def reset(self):
        self.up_shares=0; self.up_avg_price=0; self.down_shares=0; self.down_avg_price=0; self._fc_up=0; self._fc_dn=0
###############################################################################
# 11 — PNL TRACKER
###############################################################################
class PnLTracker:
    __slots__=('bankroll','initial_bankroll','total_spread_captured','total_rebates','total_realized_pnl','n_fills','n_rounds','n_shadow_fills','_fill_pnls','_start_ts')
    def __init__(self, ib):
        self.bankroll=ib; self.initial_bankroll=ib; self.total_spread_captured=0.0; self.total_rebates=0.0
        self.total_realized_pnl=0.0; self.n_fills=0; self.n_rounds=0; self.n_shadow_fills=0
        self._fill_pnls=deque(maxlen=1000); self._start_ts=time.time()
    def record_fill(self,fp,mid,sh,side,reb,shadow=False):
        sp=(mid-fp)*sh if side=="BUY" else (fp-mid)*sh
        tp=sp+reb; self.total_spread_captured+=max(0,sp); self.total_rebates+=reb
        self.total_realized_pnl+=tp; self.bankroll+=tp; self.n_fills+=1
        if shadow: self.n_shadow_fills+=1
        self._fill_pnls.append(tp); return tp
    @property
    def net_pnl(self): return self.total_realized_pnl
    def compute_mtm(self,inv,um,dm): return self.bankroll+inv.unrealized_pnl(um,dm)
    @property
    def total_pnl_pct(self): return (self.bankroll-self.initial_bankroll)/self.initial_bankroll*100 if self.initial_bankroll >0 else 0
    @property
    def sharpe(self):
        if len(self._fill_pnls) <10: return 0.0
        v=list(self._fill_pnls); n=len(v); m=sum(v)/n; var=sum((x-m)**2 for x in v)/n; s=math.sqrt(var)
        return (m/(s+1e-9))*math.sqrt(n) if s >1e-12 else 0.0
    @property
    def win_rate(self): return sum(1 for p in self._fill_pnls if p >0)/len(self._fill_pnls) if self._fill_pnls else 0.0
    def log_round(self,rn,rp,inv,um,dm):
        self.n_rounds+=1; mtm=self.compute_mtm(inv,um,dm); ur=inv.unrealized_pnl(um,dm)
        _log_sep2()
        _get_log().info("ROUND %d | Real:%s | Net:%s | MtM:$%.4f",rn,_fmt_dollar(rp),_fmt_dollar(self.net_pnl),mtm)
        _get_log().info("  Spr:%s | Reb:%s | Unr:%s",_fmt_dollar(self.total_spread_captured),_fmt_dollar(self.total_rebates),_fmt_dollar(ur))
        _get_log().info("  UP=%.2fsh DN=%.2fsh | Fills:%d(shd:%d) | WR:%.0f%% | Sharpe:%.2f",inv.up_shares,inv.down_shares,self.n_fills,self.n_shadow_fills,self.win_rate*100,self.sharpe)
        _log_sep2()
###############################################################################
# 12 — VOLATILITY ORACLE + CIRCUIT BREAKER
###############################################################################
@dataclass
class VolatilityOracle:
    xrp_prices:deque=field(default_factory=lambda:deque(maxlen=500))
    btc_prices:deque=field(default_factory=lambda:deque(maxlen=500))
    xrp_current:Optional[float]=None; btc_current:Optional[float]=None
    xrp_connected:bool=False; btc_connected:bool=False; xrp_ticks:int=0; btc_ticks:int=0
    cb_tripped:bool=False; cb_trip_ts:float=0.0; cb_trip_count:int=0
    vol_window_seconds: float = 5.0
    def update_xrp(self,p): self.xrp_current=p; self.xrp_prices.append((time.time(),p)); self.xrp_ticks+=1
    def update_btc(self,p): self.btc_current=p; self.btc_prices.append((time.time(),p)); self.btc_ticks+=1
    def _mv(self,prices,w):
        if len(prices) <2: return 0.0
        c=time.time()-w; ww=[p for t,p in prices if t >=c]
        if len(ww) <2: return 0.0
        return (max(ww)-min(ww))/min(ww) if min(ww) >0 else 0.0
    def check_cb(self,cfg):
        if self.cb_tripped:
            if time.time()-self.cb_trip_ts <cfg.circuit_breaker_cooldown_sec: return None
            self.cb_tripped=False
        w=cfg.vol_window_seconds
        xv=self._mv(self.xrp_prices,w)
        if xv >cfg.volatility_threshold_pct: return self._trip(f"XRP={xv:.4f}",xv)
        bv=self._mv(self.btc_prices,w)
        if bv >cfg.volatility_threshold_pct: return self._trip(f"BTC={bv:.4f}",bv)
        return None
    def _trip(self,tr,v):
        self.cb_tripped=True; self.cb_trip_ts=time.time(); self.cb_trip_count+=1
        _get_log().warning("[CB] TRIPPED %s >%.1f%% #%d",tr,v*100,self.cb_trip_count); return tr
    @property
    def is_cooling(self): return self.cb_tripped
    @property
    def xrp_drift(self):
        if len(self.xrp_prices) <5: return 0.0
        o=self.xrp_prices[0][1]; return (self.xrp_current-o)/o if o >0 and self.xrp_current else 0.0
    def is_xrp_stale(self,th=10.0): return (not self.xrp_prices) or (time.time()-self.xrp_prices[-1][0]) >th
_vol_oracle=VolatilityOracle()
async def _binance_ws_loop(uri,sym,cfg):
    try: import websockets
    except: return
    bk=cfg.binance_reconnect_base_s
    while True:
        try:
            async with websockets.connect(uri,ping_interval=None,ping_timeout=None,open_timeout=15,max_size=2**18) as ws:
                if sym=="XRP": _vol_oracle.xrp_connected=True
                else: _vol_oracle.btc_connected=True
                bk=cfg.binance_reconnect_base_s
                async def _ping():
                    while True:
                        await asyncio.sleep(cfg.binance_ping_interval_s)
                        try: await ws.ping()
                        except: break
                pf=asyncio.ensure_future(_ping())
                try:
                    async for raw in ws:
                        try:
                            t=raw.decode("utf-8") if isinstance(raw,bytes) else raw
                            p=float(json.loads(t).get("c",0))
                            if p>0:
                                if sym=="XRP": _vol_oracle.update_xrp(p)
                                else: _vol_oracle.update_btc(p)
                        except: pass
                finally:
                    if sym=="XRP": _vol_oracle.xrp_connected=False
                    else: _vol_oracle.btc_connected=False
                    pf.cancel()
                    try: await pf
                    except: pass
        except asyncio.CancelledError: return
        except: await asyncio.sleep(bk); bk=min(bk*2,cfg.binance_reconnect_max_s)
###############################################################################
# 13 — ORDER MANAGER
###############################################################################
class OrderManager:
    def __init__(self, cfg, clob, audit):
        self._cfg=cfg; self._clob=clob; self._audit=audit
    async def execute_cycle(self, desired, tu, td):
        cfg=self._cfg; nc=0; np=0; db=cfg.deadband_cents/100
        active=dict(self._clob._active_orders)
        ak={}
        for oid,info in active.items():
            tk=info.get("token_id",""); bk="UP" if tk==tu else ("DOWN" if tk==td else "?")
            ak[(bk,info["side"],info.get("band",0))]=(oid,info)
        dk={}
        for q in desired: dk[(q.book,q.side,q.band_idx)]=q
        cancel=[]
        for key,(oid,info) in ak.items():
            want=dk.get(key)
            if want is None: cancel.append(oid)
            elif abs(info["price"]-want.price) >db: cancel.append(oid)
        for oid in cancel:
            if await self._clob.cancel_order(oid,self._audit): nc+=1
        sa=set()
        for oid,info in self._clob._active_orders.items():
            tk=info.get("token_id",""); bk="UP" if tk==tu else ("DOWN" if tk==td else "?")
            sa.add((bk,info["side"],info.get("band",0)))
        for key,q in dk.items():
            if key not in sa:
                tid=tu if q.book=="UP" else td
                if await self._clob.post_limit_order(tid,q.side,q.price,q.size_usd,q.band_idx,self._audit): np+=1
        return nc,np
###############################################################################
# 16 — MONTE CARLO VALIDATOR (5000 sims)
###############################################################################
class MonteCarloValidator:
    """Roda 5000 simulações offline da estratégia MM antes do main loop.
    Cada simulação:
      - Gera rounds_per_sim rounds de 5 min com orderbooks sintéticos
      - Aplica SweetSpotPricer → ShadowFillEngine (probabilístico)
      - Tracked: PnL por sim, max drawdown, Sharpe, win rate
    GO/NO-GO: Sharpe mediano > mc_min_sharpe E maxDD mediano < mc_max_drawdown_pct.
    """
    def __init__(self, cfg: BotConfig):
        self._cfg = cfg
    def run(self) -> Dict[str, Any]:
        """Executa mc_sims simulações. Retorna report dict + GO/NO-GO."""
        cfg = self._cfg
        n_sims = cfg.mc_sims
        n_rounds = cfg.mc_rounds_per_sim
        ticks_per_round = int(cfg.market_duration_s / cfg.tick_interval_s)
        ticks_per_round = min(300, ticks_per_round)  # perf + limpo (HFT tick=0.1s)
        all_pnls: List[float] = []
        all_sharpes: List[float] = []
        all_max_dds: List[float] = []
        all_win_rates: List[float] = []
        _get_log().info("[MC] Starting Monte Carlo: %d sims × %d rounds × %d ticks...",
                        n_sims, n_rounds, ticks_per_round)
        t0 = time.time()
        for sim_i in range(n_sims):
            bk = cfg.bankroll_demo
            peak = bk
            max_dd = 0.0
            fill_pnls: List[float] = []
            for _ in range(n_rounds):
                mid = 0.50 + random.gauss(0, 0.03)
                mid = max(0.10, min(0.90, mid))
                spread = max(0.01, random.gauss(0.02, 0.005))
                for _ in range(ticks_per_round):
                    mid += random.gauss(0, 0.001)
                    mid = max(0.10, min(0.90, mid))
                    if random.random() < cfg.shadow_fill_prob_base * 0.3:
                        offset = random.choice([c / 100.0 for c in cfg.band_offsets_cents])
                        fp = mid - offset
                        slip = random.uniform(0, cfg.shadow_max_slippage_pct)
                        fp *= (1 + slip)
                        shares = random.choice(list(cfg.band_sizes_usd)) / (fp + 1e-9)
                        spread_pnl = (mid - fp) * shares
                        reb = polymarket_maker_rebate(shares, fp, cfg.maker_rebate_bps)
                        pnl_fill = spread_pnl + reb
                        bk += pnl_fill
                        fill_pnls.append(pnl_fill)
                    if random.random() < cfg.shadow_fill_prob_base * 0.3:
                        offset = random.choice([c / 100.0 for c in cfg.band_offsets_cents])
                        fp = mid + offset
                        slip = random.uniform(0, cfg.shadow_max_slippage_pct)
                        fp *= (1 - slip)
                        shares = random.choice(list(cfg.band_sizes_usd)) / (fp + 1e-9)
                        spread_pnl = (fp - mid) * shares
                        reb = polymarket_maker_rebate(shares, fp, cfg.maker_rebate_bps)
                        pnl_fill = spread_pnl + reb
                        bk += pnl_fill
                        fill_pnls.append(pnl_fill)
                    peak = max(peak, bk)
                    dd = (peak - bk) / (peak + 1e-9)
                    max_dd = max(max_dd, dd)
            sim_pnl = bk - cfg.bankroll_demo
            all_pnls.append(sim_pnl)
            all_max_dds.append(max_dd)
            if len(fill_pnls) >= 10:
                m = sum(fill_pnls) / len(fill_pnls)
                var = sum((x - m) ** 2 for x in fill_pnls) / len(fill_pnls)
                std = math.sqrt(var)
                sharpe = (m / (std + 1e-9)) * math.sqrt(len(fill_pnls)) if std > 1e-12 else 0.0
                wr = sum(1 for p in fill_pnls if p > 0) / len(fill_pnls)
            else:
                sharpe = 0.0
                wr = 0.0
            all_sharpes.append(sharpe)
            all_win_rates.append(wr)
        elapsed = time.time() - t0
        all_pnls.sort()
        all_sharpes.sort()
        all_max_dds.sort()
        def _pct(arr, p):
            idx = int(len(arr) * p / 100)
            return arr[min(idx, len(arr) - 1)]
        report = {
            "n_sims": n_sims,
            "elapsed_s": round(elapsed, 2),
            "pnl_mean": sum(all_pnls) / len(all_pnls),
            "pnl_median": _pct(all_pnls, 50),
            "pnl_p5": _pct(all_pnls, 5),
            "pnl_p95": _pct(all_pnls, 95),
            "sharpe_median": _pct(all_sharpes, 50),
            "sharpe_p25": _pct(all_sharpes, 25),
            "max_dd_median": _pct(all_max_dds, 50),
            "max_dd_p95": _pct(all_max_dds, 95),
            "win_rate_mean": sum(all_win_rates) / len(all_win_rates),
        }
        go = (report["sharpe_median"] >= cfg.mc_min_sharpe and
              report["pnl_median"] > 0 and
              report["max_dd_median"] < cfg.mc_max_drawdown_pct)
        report["go"] = go
        _log_sep2()
        _get_log().info("[MC_REPORT] Monte Carlo Validation — %d sims in %.1fs", n_sims, elapsed)
        _get_log().info("  PnL:    mean=%s | median=%s | p5=%s | p95=%s",
                        _fmt_dollar(report["pnl_mean"]), _fmt_dollar(report["pnl_median"]),
                        _fmt_dollar(report["pnl_p5"]), _fmt_dollar(report["pnl_p95"]))
        _get_log().info("  Sharpe: median=%.2f | p25=%.2f", report["sharpe_median"], report["sharpe_p25"])
        _get_log().info("  MaxDD:  median=%.1f%% | p95=%.1f%%", report["max_dd_median"]*100, report["max_dd_p95"]*100)
        _get_log().info("  WR:     mean=%.1f%%", report["win_rate_mean"]*100)
        verdict = "GO ✓" if go else "NO-GO ✗"
        _get_log().info("  Verdict: %s (need Sharpe >%.1f, PnL >0, DD <%.0f%%)",
                        verdict, cfg.mc_min_sharpe, cfg.mc_max_drawdown_pct*100)
        _log_sep2()
        return report
###############################################################################
# 14 — MAIN LOOP + AMM_METRICS
###############################################################################
_shutdown_flag=False; _start_time=time.time()
def log_amm(t_rem,pnl,inv,ob,na,nc,np,shadow):
    up_pct=(inv.up_value_usd/(pnl.bankroll+1e-9))*100; dn_pct=(inv.down_value_usd/(pnl.bankroll+1e-9))*100
    tag="[AMM][SHADOW]" if shadow else "[AMM]"
    _get_log().info("%s T-%.0fs | Spr=%s/%s | Inv=%.1f%%/%.1f%% | Ord=%d(C%d/P%d) | F=%d(s:%d) | Reb=%s | Net=%s | CB=%s",
        tag,t_rem,_fc(ob.up.spread),_fc(ob.down.spread),up_pct,dn_pct,na,nc,np,
        pnl.n_fills,pnl.n_shadow_fills,_fmt_dollar(pnl.total_rebates),_fmt_dollar(pnl.net_pnl),
        "TRIP" if _vol_oracle.is_cooling else "OK")
async def run_round(rn,market,cfg,clob,pricer,omgr,inv,pnl,ob,audit,wallet,
        shadow_engine=None,polymaker=None):
    global _shutdown_flag
    tu,td=MarketFinder.get_token_ids(market)
    end_ts=market.get("_end_ts",time.time()+cfg.market_duration_s); pre=pnl.bankroll
    is_shadow=cfg.shadow_mode and shadow_engine is not None
    is_sim=market.get("_simulated",False)
    mode_tag=("SHADOW" if is_shadow else ("LIVE" if cfg.live_trading else "PAPER")) + ("+SIM" if is_sim else "")
    _log_sep2()
    _get_log().info("ROUND %d | %s | %s",rn,market.get("question","?")[:50],mode_tag)
    _log_sep2()
    inv.reset(); tick_n=0; round_fills=0
    if polymaker: polymaker.reset()
    while not _shutdown_flag:
        now=time.time(); t_rem=end_ts-now
        if t_rem <=0: break
        tick_n+=1
        trigger=_vol_oracle.check_cb(cfg)
        if trigger:
            await clob.cancel_all(audit); await asyncio.sleep(cfg.circuit_breaker_cooldown_sec); continue
        if _vol_oracle.is_cooling: await asyncio.sleep(1); continue
        if (cfg.shadow_mode or cfg.live_trading) and not is_sim:
            ob.update_from_clob(clob,tu,td)
        else: ob.simulate()
        if cfg.binance_enabled and not _vol_oracle.is_xrp_stale():
            pricer.set_binance_drift(_vol_oracle.xrp_drift)
            if polymaker and _vol_oracle.xrp_current:
                polymaker.on_xrp_tick(_vol_oracle.xrp_current)
                sk = polymaker.compute_skew_adjustment(ob.up.midpoint)
                pricer.set_polymaker_skew(sk)
                pricer.set_hft_signals(0.0, 0.0, 0.0, _vol_oracle.xrp_current)  # trigger HFT
            # BUGFIX v5.3.4: sempre limpar active_orders no final do tick
            if len(clob._active_orders) > 50: await clob.cancel_all(audit)  # anti-leak
        if not cfg.shadow_mode: inv.sync_from_api(wallet,tu,td,cfg)
        quotes=pricer.compute_quotes(ob,inv.up_value_usd,inv.down_value_usd,pnl.bankroll)
        nc,np_=await omgr.execute_cycle(quotes,tu,td)
        if is_shadow:
            for sf in shadow_engine.try_match(clob,ob.up,ob.down,tu,td):
                mid=ob.up.midpoint if sf.book=="UP" else ob.down.midpoint
                fpnl=pnl.record_fill(sf.fill_price,mid,sf.shares,sf.side,sf.rebate,shadow=True)
                if sf.side=="BUY": inv.record_buy(sf.book,sf.fill_price,sf.shares)
                else: inv.record_sell(sf.book,sf.shares)
                round_fills+=1
                _get_log().info("[T-%.0fs][SHD_FILL] %s %s | %.2f@%s(ord:%s) | slip=%.2f%% | lat=%dms | PnL=%s",
                    t_rem,sf.book,sf.side,sf.shares,_fc(sf.fill_price),_fc(sf.order_price),sf.slippage_pct*100,int(sf.latency_ms),_fmt_dollar(fpnl))
                audit.log_fill(sf.side,sf.token_id,sf.fill_price,sf.shares,fpnl,sf.rebate,shadow=True)
        elif cfg.live_trading:
            for bk,tk in [("UP",tu),("DOWN",td)]:
                for fill in await clob.get_fills(tk):
                    fp=float(fill.get("price",0)); sz=float(fill.get("size",0)); side=fill.get("side","BUY")
                    if fp <=0 or sz <=0: continue
                    mid=ob.up.midpoint if bk=="UP" else ob.down.midpoint
                    reb=polymarket_maker_rebate(sz,fp,cfg.maker_rebate_bps)
                    fpnl=pnl.record_fill(fp,mid,sz,side,reb)
                    if side=="BUY": inv.record_buy(bk,fp,sz)
                    else: inv.record_sell(bk,sz)
                    round_fills+=1; audit.log_fill(side,tk,fp,sz,fpnl,reb)
        else:
            if random.random() <.15:
                for q in quotes[:2]:
                    if random.random() <.3:
                        mid=ob.up.midpoint if q.book=="UP" else ob.down.midpoint
                        sh=q.size_usd/(q.price+1e-9); reb=polymarket_maker_rebate(sh,q.price,cfg.maker_rebate_bps)
                        fpnl=pnl.record_fill(q.price,mid,sh,q.side,reb)
                        if q.side=="BUY": inv.record_buy(q.book,q.price,sh)
                        else: inv.record_sell(q.book,sh)
                        round_fills+=1
        if tick_n%cfg.log_every_n_ticks==0:
            log_amm(t_rem,pnl,inv,ob,len(clob._active_orders),nc,np_,is_shadow)
        await asyncio.sleep(cfg.tick_interval_s)
    await clob.cancel_all(audit)
    pnl.log_round(rn,pnl.bankroll-pre,inv,ob.up.midpoint,ob.down.midpoint)
    return pnl.bankroll
###############################################################################
# 15 — SHUTDOWN + ENTRY
###############################################################################
async def heartbeat_loop(cc,cfg):
    hid=None; err=0
    while True:
        try:
            await asyncio.sleep(cfg.heartbeat_interval_s)
            if not cc: continue
            snap=hid; resp=await asyncio.get_running_loop().run_in_executor(None,lambda:cc.post_heartbeat(snap))
            nid=None
            if isinstance(resp,dict): nid=resp.get("heartbeat_id") or resp.get("id")
            elif hasattr(resp,"heartbeat_id"): nid=resp.heartbeat_id
            if nid: hid=nid
            err=0
        except asyncio.CancelledError: return
        except: err+=1
        if err>=cfg.heartbeat_max_errors: await asyncio.sleep(cfg.heartbeat_interval_s*6); err=0
async def main():
    global _shutdown_flag,_start_time; _start_time=time.time()
    try: cfg=BotConfig.from_env_and_secrets("secrets.txt")
    except FileNotFoundError: cfg=BotConfig()
    except ValueError as e: print(f"[FATAL] {e}"); return
    init_logging(cfg.log_file); audit=AuditLogger(cfg.audit_file)
    def _sig():
        global _shutdown_flag; _shutdown_flag=True; _get_log().info("[SIGNAL] Shutdown...")
    loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):
        try: loop.add_signal_handler(sig,_sig)
        except: pass
    wallet=WalletManager(cfg); bankroll=wallet.get_balance_usdc()
    clob=PolymarketCLOB(cfg); pricer=SweetSpotPricer(cfg); inv=InventoryManager()
    pnl_tracker=PnLTracker(bankroll); ob=DualOrderBook(); omgr=OrderManager(cfg,clob,audit)
    shadow_engine=ShadowFillEngine(cfg) if cfg.shadow_mode else None
    polymaker=PolyMakerBridge(cfg)
    mode_str="SHADOW" if cfg.shadow_mode else ("LIVE" if cfg.live_trading else "PAPER")
    _log_sep2()
    _get_log().info("XRP MARKET MAKER v5.3.4 — HFT Ultra")
    _get_log().info("POST_ONLY | %s | LMSR(b=%.0f) | Bayesian | CB",mode_str,cfg.lmsr_b)
    _log_sep2()
    _get_log().info("Mode:     %s | Bankroll: $%.4f",mode_str,bankroll)
    _get_log().info("Shadow:   lat=%dms | slip=%.1f%% | prob=%.0f%%",int(cfg.shadow_latency_ms),cfg.shadow_max_slippage_pct*100,cfg.shadow_fill_prob_base*100)
    _get_log().info("LMSR:     b=%.0f | L_max=$%.2f | EV_min=%.3f",cfg.lmsr_b,cfg.lmsr_b*math.log(2),cfg.ev_min_entry)
    _get_log().info("Bayes:    prior=%.2f | std=%.4f | \"NEVER full Kelly on 5min\"",cfg.bayesian_prior,cfg.bayesian_likelihood_std)
    _get_log().info("MC:       %d sims | GO: Sharpe >%.1f, DD <%.0f%%",cfg.mc_sims,cfg.mc_min_sharpe,cfg.mc_max_drawdown_pct*100)
    _get_log().info("Bands:    %s cents | Sizes: %s USD",cfg.band_offsets_cents,cfg.band_sizes_usd)
    _get_log().info("Defense:  vol=%.0f%% | cb=%.0fs | deadband=%.4fc",cfg.volatility_threshold_pct*100,cfg.circuit_breaker_cooldown_sec,cfg.deadband_cents)
    _log_sep2()
    audit.log_event("BOOT",f"v5.3.4|{mode_str}|bk=${bankroll:.4f}")
    # ── # 16: Monte Carlo GO/NO-GO ──────────────────────────────────────────
    if cfg.mc_run_at_boot:
        mc = MonteCarloValidator(cfg)
        report = mc.run()
        if not report["go"]:
            _get_log().warning("[MC] *** NO-GO *** Strategy did not pass validation. Continuing in shadow mode for observation.")
            audit.log_event("MC","NO-GO — running for observation only")
    # ── Background tasks ───────────────────────────────────────────────────
    tasks: List[asyncio.Task]=[]
    tasks.append(asyncio.create_task(config_hot_reload_loop(cfg),name="config_reload"))
    if cfg.binance_enabled:
        tasks.append(asyncio.create_task(_binance_ws_loop(cfg.binance_ws_uri,"XRP",cfg),name="bnc_xrp"))
        tasks.append(asyncio.create_task(_binance_ws_loop(cfg.binance_btc_ws_uri,"BTC",cfg),name="bnc_btc"))
    if cfg.live_trading and clob._client and not cfg.shadow_mode:
        tasks.append(asyncio.create_task(heartbeat_loop(clob._client,cfg),name="heartbeat"))
    if cfg.binance_enabled:
        _get_log().info("[BOOT] Waiting for Binance...")
        for _ in range(30):
            if _vol_oracle.xrp_current: break
            await asyncio.sleep(0.1)
        if _vol_oracle.xrp_current: _get_log().info("[BOOT] Binance OK: XRP=$%.5f",_vol_oracle.xrp_current)
        else: _get_log().warning("[BOOT] No Binance data")
    rn=0
    try:
        while not _shutdown_flag:
            rn+=1; _get_log().info("[ROUND %d] Searching XRP market...",rn)
            market=None; attempts=0
            while market is None and not _shutdown_flag:
                market=MarketFinder.find_xrp_5min(cfg)
                if not market: attempts+=1; _get_log().warning("[ROUND %d] No market (attempt %d) — 30s",rn,attempts); await asyncio.sleep(30)
            if _shutdown_flag: break
            if cfg.live_trading and not cfg.shadow_mode:
                lb=wallet.get_balance_usdc()
                if lb >0: pnl_tracker.bankroll=lb
            try: await run_round(rn,market,cfg,clob,pricer,omgr,inv,pnl_tracker,ob,audit,wallet,shadow_engine,polymaker)
            except Exception as e:
                _get_log().error("[ROUND %d] Error: %s\n%s",rn,e,traceback.format_exc())
                await clob.cancel_all(audit)
            _get_log().info("Wait %.0fs...",cfg.between_markets_s); await asyncio.sleep(cfg.between_markets_s)
    except Exception as e:
        _get_log().critical("[FATAL] %s\n%s",e,traceback.format_exc())
    finally:
        _get_log().info("[SHUTDOWN] Graceful shutdown...")
        try: nc=await clob.cancel_all(audit); _get_log().info("[SHUTDOWN] %d cancelled",nc)
        except: pass
        for t in tasks: t.cancel()
        for t in tasks:
            try: await t
            except asyncio.CancelledError: pass
        mtm=pnl_tracker.compute_mtm(inv,ob.up.midpoint,ob.down.midpoint); up_str=_uptime(_start_time)
        _log_sep2()
        _get_log().info("SHUTDOWN — %s",mode_str)
        _get_log().info("  Cash:$%.4f | MtM:$%.4f | Net:%s | Spr:%s | Reb:%s",pnl_tracker.bankroll,mtm,_fmt_dollar(pnl_tracker.net_pnl),_fmt_dollar(pnl_tracker.total_spread_captured),_fmt_dollar(pnl_tracker.total_rebates))
        _get_log().info("  Fills:%d(shd:%d) | API:%d | Rounds:%d | CB:%d | WR:%.0f%% | Sharpe:%.2f | %s",
            pnl_tracker.n_fills,pnl_tracker.n_shadow_fills,clob.api_calls,pnl_tracker.n_rounds,_vol_oracle.cb_trip_count,pnl_tracker.win_rate*100,pnl_tracker.sharpe,up_str)
        _get_log().info("  Bayesian: P(UP)=%.3f (%d updates) | LMSR: p_up=%.3f",
            polymaker.bayesian.p_up,polymaker.bayesian._updates,polymaker.lmsr.price_up())
        _log_sep2()
        audit.log_shutdown(pnl_tracker.bankroll,pnl_tracker.net_pnl,up_str)
        if _log_listener: _log_listener.stop()
if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt:
        if log: log.info("STOPPED (Ctrl+C)")
        else: print("STOPPED")
# ✅ v5.3.4 — HFT Ultra aplicado com sucesso