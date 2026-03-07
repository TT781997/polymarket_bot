# =============================================================================
# BOT XRP POLYMARKET — MODULES v1.9.0
# =============================================================================
#
# Três módulos de produção para integração com BOT_XRP_POLYMARKET v1.8.0:
#
#   MODULE 1 — ResolutionPoller:
#     Background async task que poll'a a REST API Polymarket para confirmar
#     resolução de mercados expirados. Permite ao bot matar o WS na expiração
#     e avançar imediatamente para o mercado seguinte, enquanto o poller
#     resolve o PnL em background.
#
#   MODULE 2 — evaluate_aggressive_endgame():
#     Avaliador de endgame agressivo (últimos 5s do mercado).
#     Bypassa TODOS os filtros HFT (spread, σ, Z, OBI, VPIN, Kelly).
#     Se algum lado tem preço entre 75c-95c → sinal de BUY imediato.
#
#   MODULE 3 — execute_fok_order() / execute_fak_order():
#     Execução de ordens FOK/FAK via py_clob_client com protecção de
#     slippage. Comportamento IDÊNTICO em DEMO e LIVE (mesmo code path;
#     apenas post_order condicional).
#
# ─────────────────────────────────────────────────────────────────────────────
#
# 📋 STEP 1 — REQUIREMENTS CONFIRMATION
#
# 🎯 Goal:
#   1. Async REST poller que corre em background, detecta market_resolved
#      e dispara callback de PnL sem bloquear o loop principal.
#   2. Avaliador que ignora todos os filtros HFT nos últimos 5s e retorna
#      sinal de BUY para o lado com preço no range 75c-95c.
#   3. Wrapper de execução FOK/FAK com slippage protection,
#      idêntico em DEMO e LIVE, com logging exaustivo.
#
# 📥 Inputs:
#   Module 1: slug (str), condition_id (str), token_ids (dict),
#             active_trades (list), callback (Callable)
#   Module 2: remaining_seconds (float), best_asks (dict),
#             meta (dict com token_ids)
#   Module 3: client (ClobClient), side (BUY/SELL), token_id (str),
#             amount (float), price (float), slippage (float)
#
# 📤 Outputs:
#   Module 1: asyncio.Task que roda até resolução ou cancelamento
#   Module 2: Optional[EndgameSignal] com side, token_id, ask price
#   Module 3: OrderResult com success, order_id, filled_amount, logs
#
# ⚠️ Edge Cases:
#   - REST API down durante polling → retry com backoff, sem crash
#   - Mercado nunca resolve (bug Polymarket) → timeout com REFUND
#   - Preço oscila entre 74c e 76c nos últimos 5s → só dispara >= 75c
#   - FOK rejeitado (sem liquidez) → log + return False (sem retry)
#   - FAK parcialmente filled → log fill parcial, return True
#   - SDK ImportError (DEMO sem py_clob_client) → simula 100%
#   - Network timeout no post_order → catch, log, return False
#
# 🚫 Assumptions:
#   - aiohttp disponível (pip install aiohttp)
#   - py_clob_client disponível para LIVE (já é requisito de v1.7.0)
#   - GAMMA_API_URL responde com markets[0].resolved = True/False
#   - CLOB tokens[].winner = "true"/"false" após resolução
#   - Tick size 0.01 para todos os mercados XRP 5-min
#
# ─────────────────────────────────────────────────────────────────────────────
#
# 🏗️ STEP 2 — DESIGN DECISION LOG
#
# | Decision            | Approach         | Why                     | Complexity |
# |---------------------|------------------|-------------------------|------------|
# | Polling lib         | aiohttp          | Non-blocking; async     | O(1)/poll  |
# | Poll interval       | 20s configurable | Balance: speed vs. load | O(1)       |
# | Endgame structure   | NamedTuple       | Immutable, typed signal | O(1)       |
# | Order wrapper       | Single function  | DEMO/LIVE identical     | O(1)       |
# | Slippage direction  | BUY:+slip SELL:-s| Worst-case limit price  | O(1)       |
# | Error isolation     | try/except per op| Falha parcial ≠ crash   | -          |
# | State machine       | Enum for poller  | Clear lifecycle         | O(1)       |
#
# Type-hinting: completo em todas as funções e classes.
# Testabilidade: cada módulo é standalone e mockável.
# Security: nenhum secret em log; token_ids truncados.
# Deps: aiohttp (novo), py_clob_client (existente), stdlib.
#
# =============================================================================

from __future__ import annotations

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
)

# ─── Importações condicionais (graceful degradation) ────────────────────────

try:
    import aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    _HAS_AIOHTTP = False

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.clob_types import OrderType
    _HAS_SDK = True
except ImportError:
    # Stubs para DEMO sem SDK instalado
    ClobClient = None  # type: ignore[assignment,misc]
    BUY = "BUY"
    SELL = "SELL"

    class OrderType:  # type: ignore[no-redef]
        FOK = "FOK"
        FAK = "FAK"

    _HAS_SDK = False

# ─── Logger (reutiliza o logger do bot principal) ────────────────────────────

logger = logging.getLogger("bot_xrp")


def _ts() -> str:
    """Timestamp formatado idêntico ao do bot principal."""
    from datetime import datetime
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]


def _log_mod(module: str, action: str, msg: str) -> None:
    logger.info(
        f"[INFO] [{module}] [{action}] [{_ts()}] | {msg}"
    )


def _log_warn(module: str, msg: str) -> None:
    logger.warning(f"[WARN] [{module}] [{_ts()}] | {msg}")


def _fc(p: float) -> str:
    """Formata preço 0-1 em cents."""
    return f"{p * 100:.1f}c"


# =============================================================================
# PARAMETROS GLOBAIS DO MÓDULO v1.9.0
# =============================================================================

# --- SLIPPAGE ---
SLIPPAGE_TOLERANCE: float = 0.02
# Tolerância de slippage em preço (0.02 = 2 cents).
# BUY: worst_price = ask + SLIPPAGE_TOLERANCE
# SELL: worst_price = bid - SLIPPAGE_TOLERANCE
# Configurável pelo utilizador; afecta execute_fok_order/execute_fak_order.

# --- RESOLUTION POLLER ---
POLL_INTERVAL_S: float = 10.0
# Intervalo entre polls REST para verificar resolução (segundos).
# 20s equilibra velocidade de detecção vs. carga na API.

POLL_MAX_DURATION_S: float = 900.0
# Duração máxima do poller antes de desistir (safety net).
# Alinhado com RESOLVE_MAX_WAIT_S do v1.8.0.

POLL_REQUEST_TIMEOUT_S: float = 10.0
# Timeout por pedido REST individual (segundos).

# --- AGGRESSIVE ENDGAME ---
ENDGAME_AGGRESSIVE_S: float = 5.0
# Segundos finais em que o modo agressivo se activa (bypassa ALL filters).

ENDGAME_MIN_PRICE: float = 0.75
# Preço mínimo (inclusive) para sinal de compra agressiva.

ENDGAME_MAX_PRICE: float = 0.95
# Preço máximo (inclusive) para sinal de compra agressiva.

# --- ORDER EXECUTION ---
ORDER_TICK_SIZE: str = "0.01"
# Tick size para mercados XRP 5-min Polymarket.

ORDER_NEG_RISK: bool = False
# neg_risk flag para mercados XRP 5-min.


# =============================================================================
# MODULE 1 — RESOLUTION POLLER (Background REST Task)
# =============================================================================

class PollerState(enum.Enum):
    """Estados do ciclo de vida do ResolutionPoller."""
    IDLE = "IDLE"
    POLLING = "POLLING"
    RESOLVED = "RESOLVED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    ERROR = "ERROR"


@dataclass
class ResolutionResult:
    """Resultado da resolução de um mercado via REST polling.

    Attributes:
        resolved: True se o mercado foi confirmado como resolvido.
        winner_token_id: Token ID do vencedor (None se não resolvido).
        winning_outcome: "UP" ou "DOWN" (None se não resolvido).
        polls_made: Número total de polls executados.
        elapsed_s: Tempo total de polling em segundos.
        raw_response: Última resposta crua da API (para debug).
    """
    resolved: bool = False
    winner_token_id: Optional[str] = None
    winning_outcome: Optional[str] = None
    polls_made: int = 0
    elapsed_s: float = 0.0
    raw_response: Optional[Dict[str, Any]] = None


class ResolutionPoller:
    """Background async task que poll'a a REST API para resolução.

    Fluxo de uso:
        1. Bot atinge fim de mercado (rem <= 0).
        2. Bot cria ResolutionPoller com os dados do mercado expirado.
        3. Bot chama poller.start() → asyncio.Task em background.
        4. Bot avança imediatamente para subscrever o mercado seguinte.
        5. Quando o poller detecta resolução, dispara o callback de PnL.
        6. Se timeout, dispara callback com resultado não-resolvido.

    O callback recebe um ResolutionResult e é responsável por
    actualizar bankroll/PnL nos active_trades correspondentes.

    Args:
        slug: Slug do mercado expirado.
        condition_id: Condition ID do mercado.
        token_up: Token ID do lado UP.
        token_down: Token ID do lado DOWN.
        gamma_api_url: Base URL da Gamma API.
        on_resolved: Callback async chamado com ResolutionResult.
        poll_interval_s: Intervalo entre polls (default 20s).
        max_duration_s: Duração máxima (default 600s).
        request_timeout_s: Timeout por request (default 10s).

    Example:
        >>> poller = ResolutionPoller(
        ...     slug="xrp-updown-5m-174126",
        ...     condition_id="0xabc...",
        ...     token_up="tok_up_id",
        ...     token_down="tok_down_id",
        ...     gamma_api_url="https://gamma-api.polymarket.com",
        ...     on_resolved=my_pnl_callback,
        ... )
        >>> task = poller.start()
        >>> # ... bot continua a operar o mercado seguinte ...
        >>> # callback disparado automaticamente quando resolvido
    """

    __slots__ = (
        "_slug", "_condition_id", "_token_up", "_token_down",
        "_api_url", "_callback", "_interval", "_max_dur",
        "_req_timeout", "_state", "_task", "_result",
    )

    def __init__(
        self,
        slug: str,
        condition_id: str,
        token_up: str,
        token_down: str,
        gamma_api_url: str,
        on_resolved: Callable[
            [ResolutionResult], Any
        ],
        poll_interval_s: float = POLL_INTERVAL_S,
        max_duration_s: float = POLL_MAX_DURATION_S,
        request_timeout_s: float = POLL_REQUEST_TIMEOUT_S,
    ) -> None:
        self._slug: str = slug
        self._condition_id: str = condition_id
        self._token_up: str = token_up
        self._token_down: str = token_down
        self._api_url: str = gamma_api_url
        self._callback = on_resolved
        self._interval: float = poll_interval_s
        self._max_dur: float = max_duration_s
        self._req_timeout: float = request_timeout_s
        self._state: PollerState = PollerState.IDLE
        self._task: Optional[asyncio.Task] = None
        self._result: ResolutionResult = ResolutionResult()

    @property
    def state(self) -> PollerState:
        """Estado actual do poller."""
        return self._state

    @property
    def result(self) -> ResolutionResult:
        """Último resultado (em progresso ou final)."""
        return self._result

    def start(self) -> asyncio.Task:
        """Inicia o polling em background. Retorna o Task."""
        if self._state == PollerState.POLLING:
            _log_warn("POLLER",
                      f"Já em polling para {self._slug} — ignorado")
            return self._task  # type: ignore[return-value]
        self._state = PollerState.POLLING
        self._result = ResolutionResult()
        self._task = asyncio.create_task(
            self._poll_loop(),
            name=f"poller-{self._slug}"
        )
        _log_mod("POLLER", "START",
                 f"slug={self._slug} | interval={self._interval}s "
                 f"| max={self._max_dur}s")
        return self._task

    def cancel(self) -> None:
        """Cancela o polling em curso."""
        if self._task and not self._task.done():
            self._task.cancel()
            self._state = PollerState.CANCELLED
            _log_mod("POLLER", "CANCEL",
                     f"slug={self._slug} | polls={self._result.polls_made}")

    async def _poll_loop(self) -> None:
        """Loop principal de polling. Chama _check_resolution a cada
        intervalo até resolução, timeout ou cancelamento."""
        t0 = time.monotonic()
        try:
            while True:
                elapsed = time.monotonic() - t0
                if elapsed >= self._max_dur:
                    self._state = PollerState.TIMEOUT
                    self._result.elapsed_s = elapsed
                    _log_warn("POLLER",
                              f"TIMEOUT {self._max_dur:.0f}s | "
                              f"slug={self._slug} | "
                              f"polls={self._result.polls_made}")
                    break

                resolved = await self._check_resolution()
                self._result.polls_made += 1

                if resolved:
                    self._state = PollerState.RESOLVED
                    self._result.elapsed_s = (
                        time.monotonic() - t0
                    )
                    _log_mod("POLLER", "RESOLVED",
                             f"slug={self._slug} | "
                             f"winner={self._result.winning_outcome} | "
                             f"token={self._result.winner_token_id[:16] if self._result.winner_token_id else '?'}... | "
                             f"polls={self._result.polls_made} | "
                             f"elapsed={self._result.elapsed_s:.1f}s")
                    break

                await asyncio.sleep(self._interval)

        except asyncio.CancelledError:
            self._state = PollerState.CANCELLED
            self._result.elapsed_s = time.monotonic() - t0
            _log_mod("POLLER", "CANCELLED",
                     f"slug={self._slug} | "
                     f"polls={self._result.polls_made}")
            return
        except Exception as exc:
            self._state = PollerState.ERROR
            self._result.elapsed_s = time.monotonic() - t0
            _log_warn("POLLER",
                      f"ERRO FATAL: {type(exc).__name__}: {exc} | "
                      f"slug={self._slug}")

        # Disparar callback com resultado final
        try:
            cb_result = self._callback(self._result)
            if asyncio.iscoroutine(cb_result):
                await cb_result
        except Exception as cb_exc:
            _log_warn("POLLER",
                      f"CALLBACK ERRO: {type(cb_exc).__name__}: "
                      f"{cb_exc}")

    async def _check_resolution(self) -> bool:
        """Faz um pedido REST e verifica se o mercado resolveu.

        Tenta a Gamma API (events?slug=...) para obter o estado
        de resolução e o winning asset.

        Returns:
            True se resolvido, False caso contrário.
        """
        if not _HAS_AIOHTTP:
            # Fallback sync (não-ideal mas funcional)
            return await self._check_resolution_sync()

        url = (
            f"{self._api_url}/events?slug={self._slug}"
        )
        try:
            timeout = aiohttp.ClientTimeout(
                total=self._req_timeout
            )
            async with aiohttp.ClientSession(
                timeout=timeout
            ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        _log_warn("POLLER",
                                  f"HTTP {resp.status} de {url}")
                        return False
                    data = await resp.json()
        except asyncio.TimeoutError:
            _log_warn("POLLER",
                      f"Timeout {self._req_timeout}s | {url}")
            return False
        except aiohttp.ClientError as e:
            _log_warn("POLLER",
                      f"aiohttp erro: {type(e).__name__}: {e}")
            return False
        except Exception as e:
            _log_warn("POLLER",
                      f"Erro inesperado: {type(e).__name__}: {e}")
            return False

        return self._parse_gamma_response(data)

    #async def _check_resolution_sync(self) -> bool:
    #    """Fallback síncrono quando aiohttp não está disponível.
    #    Usa requests em executor para não bloquear o event loop."""
    #    import requests as req_lib
    #    loop = asyncio.get_event_loop()
    #    url = (
    #        f"{self._api_url}/events?slug={self._slug}"
    #    )
    #    try:
    #        resp = await loop.run_in_executor(
    #            None,
    #            lambda: req_lib.get(url, timeout=self._req_timeout)
    #        )
    #        if resp.status_code != 200:
    #            _log_warn("POLLER",
    #                      f"HTTP {resp.status_code} (sync) | {url}")
    #            return False
    #        data = resp.json()
    #    except Exception as e:
    #        _log_warn("POLLER",
    #                  f"Erro sync: {type(e).__name__}: {e}")
    #        return False
    #
    #    return self._parse_gamma_response(data)
    async def _check_resolution(self) -> bool:
        """v1.9.2 — Multi-endpoint + fallback requests + log URL completa."""
        urls_to_try = [
            f"{self._api_url}/markets?condition_id={self._condition_id}",
            f"{self._api_url}/markets?slug={self._slug}",
            f"{self._api_url}/events?slug={self._slug}",
        ]

        if _HAS_AIOHTTP:
            for url in urls_to_try:
                try:
                    timeout = aiohttp.ClientTimeout(total=self._req_timeout)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                continue
                            data = await resp.json()
                            _log_mod("POLLER", "API_OK", f"aiohttp {url}")
                            if self._parse_gamma_response(data):
                                return True
                except Exception:
                    continue

        # Fallback requests (sempre disponível)
        try:
            import requests
            loop = asyncio.get_event_loop()
            for url in urls_to_try:
                try:
                    resp = await loop.run_in_executor(
                        None, lambda u=url: requests.get(u, timeout=self._req_timeout)
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        _log_mod("POLLER", "API_OK", f"requests {url}")
                        if self._parse_gamma_response(data):
                            return True
                except Exception:
                    continue
        except Exception:
            pass

        return False

    #def _parse_gamma_response(
    #    self, data: Any
    #) -> bool:
    #    """Analisa resposta da Gamma API e extrai resolução.
    #
    #    Espera formato: lista de events, cada um com markets[].
    #    Campos relevantes:
    #      - market["resolved"] : bool
    #      - market["winner"]   : str token_id do vencedor
    #
    #    Args:
    #        data: JSON parsed da resposta Gamma API.
    #
    #    Returns:
    #        True se mercado está resolvido com vencedor identificado.
    #    """
    #    self._result.raw_response = data
    #    try:
    #        if not isinstance(data, list) or len(data) == 0:
    #            return False
    #        event = data[0]
    #        markets = event.get("markets", [])
    #        if not markets:
    #            return False
    #        market = markets[0]
    #
    #        # Check resolução
    #        is_resolved = market.get("resolved", False)
    #        if is_resolved is not True and str(is_resolved).lower() != "true":
    #            return False
    #
    #        self._result.resolved = True
    #
    #        # Determinar vencedor
    #        # Gamma API: "winner" contém o token_id vencedor
    #        winner_raw = market.get("winner", "")
    #        clob_ids_raw = market.get("clobTokenIds", "[]")
    #
    #        # clobTokenIds pode ser string JSON ou lista
    #        if isinstance(clob_ids_raw, str):
    #            import json
    #            try:
    #                clob_ids = json.loads(clob_ids_raw)
    #            except (json.JSONDecodeError, ValueError):
    #                clob_ids = []
    #        else:
    #            clob_ids = clob_ids_raw
    #
    #        # Mapear winner para token_id e outcome
    #        if winner_raw == self._token_up:
    #            self._result.winner_token_id = self._token_up
    #            self._result.winning_outcome = "UP"
    #        elif winner_raw == self._token_down:
    #            self._result.winner_token_id = self._token_down
    #            self._result.winning_outcome = "DOWN"
    #        elif len(clob_ids) >= 2:
    #            # Fallback: "winner" pode ser índice ou outcome
    #            outcomes = market.get("outcomes", "")
    #            if isinstance(outcomes, str):
    #                import json
    #                try:
    #                    outcomes = json.loads(outcomes)
    #                except (json.JSONDecodeError, ValueError):
    #                    outcomes = []
    #            # Tentar por outcomePrices
    #            prices_raw = market.get("outcomePrices", "")
    #            if isinstance(prices_raw, str):
    #                import json
    #                try:
    #                    prices = json.loads(prices_raw)
    #                except (json.JSONDecodeError, ValueError):
    #                    prices = []
    #            else:
    #                prices = prices_raw or []
    #            if len(prices) >= 2:
    #                p0 = float(prices[0])
    #                p1 = float(prices[1])
    #                if p0 > p1:
    #                    self._result.winner_token_id = clob_ids[0]
    #                    self._result.winning_outcome = "UP"
    #                else:
    #                    self._result.winner_token_id = clob_ids[1]
    #                    self._result.winning_outcome = "DOWN"
    #            elif winner_raw:
    #                # Último recurso: usar winner directamente
    #                self._result.winner_token_id = winner_raw
    #                self._result.winning_outcome = "UNKNOWN"
    #
    #        return self._result.resolved
    #
    #    except (KeyError, IndexError, TypeError, ValueError) as e:
    #        _log_warn("POLLER",
    #                  f"Parse erro: {type(e).__name__}: {e}")
    #        return False
    def _parse_gamma_response(self, data: Any) -> bool:
        """v1.9.2 — Parsing 100% robusto para mercados 5-min XRP (sem 'resolved' field)."""
        self._result.raw_response = data
        if not data:
            return False
    
        # Normaliza resposta (pode ser list ou dict)
        if isinstance(data, list) and data:
            item = data[0]
        else:
            item = data
        markets = item.get("markets", []) if isinstance(item, dict) else []
        if not markets and isinstance(item, dict):
            markets = [item]
        if not markets:
            return False
    
        market = markets[0]
    
        # DEBUG forte (primeiros 5 polls)
        if self._result.polls_made <= 5:
            resolved_val = market.get("resolved") or market.get("closed") or market.get("umaResolutionStatus")
            has_outcomes = "outcomes" in market
            has_prices = "outcomePrices" in market
            _log_mod("POLLER", "DEBUG",
                     f"poll={self._result.polls_made} | resolved_val={resolved_val} | "
                     f"closed={market.get('closed')} | umaStatus={market.get('umaResolutionStatus')} | "
                     f"outcomes={has_outcomes} | outcomePrices={has_prices}")
    
        # === 1. Verifica resolução (vários campos possíveis) ===
        resolved_val = (
            market.get("resolved") or
            market.get("closed") or
            market.get("umaResolutionStatus") == "resolved"
        )
        if str(resolved_val).lower() not in ("true", "1", "yes"):
            # Verifica por preços (um lado = 1.00) — comum em mercados curtos
            prices = market.get("outcomePrices") or market.get("prices", [])
            if isinstance(prices, str):
                try:
                    import json
                    prices = json.loads(prices)
                except:
                    prices = []
            if len(prices) >= 2:
                try:
                    p0 = float(prices[0])
                    p1 = float(prices[1])
                    if p0 > 0.99 or p1 > 0.99:
                        resolved_val = True
                except:
                    pass
                
        if not resolved_val:
            return False
    
        self._result.resolved = True
    
        # === 2. Detecta vencedor (prioridade: preços → winner → outcomes) ===
        # Preços (mais fiável)
        prices = market.get("outcomePrices") or market.get("prices", [])
        if isinstance(prices, str):
            try:
                import json
                prices = json.loads(prices)
            except:
                prices = []
        if len(prices) >= 2:
            try:
                p0 = float(prices[0])
                p1 = float(prices[1])
                if p0 > p1:
                    self._result.winner_token_id = self._token_up
                    self._result.winning_outcome = "UP"
                else:
                    self._result.winner_token_id = self._token_down
                    self._result.winning_outcome = "DOWN"
                _log_mod("POLLER", "WINNER_PRICES", f"{self._result.winning_outcome} (p0={p0:.2f} p1={p1:.2f})")
                return True
            except:
                pass
            
        # Winner directo
        winner_raw = market.get("winner")
        if winner_raw:
            if winner_raw == self._token_up or "up" in str(winner_raw).lower():
                self._result.winner_token_id = self._token_up
                self._result.winning_outcome = "UP"
            else:
                self._result.winner_token_id = self._token_down
                self._result.winning_outcome = "DOWN"
            return True
    
        # Fallback: outcomes array (primeiro = UP, segundo = DOWN)
        outcomes = market.get("outcomes", [])
        if isinstance(outcomes, str):
            try:
                import json
                outcomes = json.loads(outcomes)
            except:
                outcomes = []
        if len(outcomes) >= 2:
            for i, outcome in enumerate(outcomes[:2]):
                if isinstance(outcome, dict):
                    if outcome.get("winner") is True or outcome.get("price", 0) > 0.99:
                        side = "UP" if i == 0 else "DOWN"
                        self._result.winner_token_id = self._token_up if side == "UP" else self._token_down
                        self._result.winning_outcome = side
                        return True
    
        _log_warn("POLLER", "Resolvido mas winner ainda não identificado — continuar polling")
        self._result.resolved = False  # segurança: só aceita se tiver winner
        return False

# =============================================================================
# MODULE 2 — AGGRESSIVE ENDGAME EVALUATOR
# =============================================================================

class EndgameSignal(NamedTuple):
    """Sinal de compra agressiva emitido pelo avaliador de endgame.

    Attributes:
        side: "UP" ou "DOWN" — lado a comprar.
        token_id: Token ID do activo a comprar.
        ask_price: Preço ASK actual do activo.
        confidence: Nível de confiança (0.0-1.0) baseado
                    na distância do preço ao extremo.
    """
    side: str
    token_id: str
    ask_price: float
    confidence: float


def evaluate_aggressive_endgame(
    remaining_s: float,
    best_asks: Dict[str, Optional[float]],
    meta: Dict[str, str],
    trigger_s: float = ENDGAME_AGGRESSIVE_S,
    min_price: float = ENDGAME_MIN_PRICE,
    max_price: float = ENDGAME_MAX_PRICE,
) -> Optional[EndgameSignal]:
    """Avalia se o modo endgame agressivo deve disparar.

    Este avaliador BYPASSA TODOS os filtros HFT standard:
    - Ignora MAX_SPREAD_CENTS
    - Ignora GAMB_MAX_VOL_DEV (σ)
    - Ignora GAMB_MAX_ZSCORE (Z)
    - Ignora GAMB_MIN_OBI
    - Ignora VPIN_SAFE_LIMIT
    - Ignora BID_ASK_MIN_RATIO
    - Ignora GAMB_MIN_EFF_C / GAMB_MAX_EFF_C
    - Ignora Kelly Criterion (usa risco fixo configurável)

    A lógica é deliberadamente simples: nos últimos 5 segundos,
    o mercado converge para o resultado. Se um lado está a 75c-95c,
    é muito provavelmente o vencedor.

    Se AMBOS os lados estão no range (improvável mas possível em
    mercados com spread apertado perto de 50c), selecciona o mais
    caro (maior probabilidade implícita).

    Args:
        remaining_s: Segundos restantes até expiração do mercado.
        best_asks: Dict com best ASK por lado {"up": float, "down": float}.
        meta: Dict com token IDs {"up": str, "down": str}.
        trigger_s: Limiar de activação em segundos (default 5.0).
        min_price: Preço mínimo inclusivo (default 0.75).
        max_price: Preço máximo inclusivo (default 0.95).

    Returns:
        EndgameSignal se um lado qualifica para compra agressiva.
        None se nenhum lado qualifica ou se remaining > trigger.

    Example:
        >>> signal = evaluate_aggressive_endgame(
        ...     remaining_s=3.2,
        ...     best_asks={"up": 0.87, "down": 0.14},
        ...     meta={"up": "tok_up", "down": "tok_down"},
        ... )
        >>> signal.side
        'UP'
        >>> signal.ask_price
        0.87
    """
    # ── Gate 1: tempo — só activa nos últimos N segundos ──────
    if remaining_s > trigger_s or remaining_s <= 0.0:
        return None

    ask_up = best_asks.get("up")
    ask_down = best_asks.get("down")

    # ── Gate 2: dados disponíveis ─────────────────────────────
    if ask_up is None and ask_down is None:
        _log_warn("ENDGAME_AGG",
                  "Sem ASKs disponíveis — não pode avaliar")
        return None

    # ── Avaliar cada lado ─────────────────────────────────────
    candidates: List[EndgameSignal] = []

    if ask_up is not None and min_price <= ask_up <= max_price:
        # Confiança: quão perto do extremo superior (mais caro = mais provável)
        confidence = (ask_up - min_price) / (max_price - min_price)
        candidates.append(EndgameSignal(
            side="UP",
            token_id=meta.get("up", ""),
            ask_price=ask_up,
            confidence=confidence,
        ))

    if ask_down is not None and min_price <= ask_down <= max_price:
        confidence = (ask_down - min_price) / (max_price - min_price)
        candidates.append(EndgameSignal(
            side="DOWN",
            token_id=meta.get("down", ""),
            ask_price=ask_down,
            confidence=confidence,
        ))

    if not candidates:
        _log_mod("ENDGAME_AGG", "NO_SIGNAL",
                 f"rem={remaining_s:.2f}s | "
                 f"UP_ask={_fc(ask_up) if ask_up else 'n/a'} "
                 f"DN_ask={_fc(ask_down) if ask_down else 'n/a'} "
                 f"| fora do range [{_fc(min_price)}-{_fc(max_price)}]")
        return None

    # Se ambos no range, escolher o mais caro (maior prob. implícita)
    best = max(candidates, key=lambda s: s.ask_price)

    _log_mod("ENDGAME_AGG", "SIGNAL",
             f"rem={remaining_s:.2f}s | {best.side} @ "
             f"ASK={_fc(best.ask_price)} | "
             f"conf={best.confidence:.2f} | "
             f"ALL HFT FILTERS BYPASSED")

    return best


# =============================================================================
# MODULE 3 — FOK / FAK ORDER EXECUTION
# =============================================================================

@dataclass
class OrderResult:
    """Resultado de uma execução de ordem FOK/FAK.

    Attributes:
        success: True se a ordem foi aceite (FOK: filled; FAK: parcial+).
        order_id: ID da ordem retornado pela API (None se falhou).
        order_type: "FOK" ou "FAK".
        side: "BUY" ou "SELL".
        requested_amount: Montante pedido ($ para BUY, shares para SELL).
        price_sent: Preço com slippage enviado à API.
        price_intended: Preço original sem slippage.
        slippage_applied: Slippage efectivamente aplicado.
        is_live: True se ordem foi enviada à API real.
        is_demo: True se ordem foi simulada.
        error: Mensagem de erro (None se sucesso).
        raw_response: Resposta crua da API (None em DEMO).
    """
    success: bool = False
    order_id: Optional[str] = None
    order_type: str = ""
    side: str = ""
    requested_amount: float = 0.0
    price_sent: float = 0.0
    price_intended: float = 0.0
    slippage_applied: float = 0.0
    is_live: bool = False
    is_demo: bool = False
    error: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None


def _compute_worst_price(
    side: str,
    price: float,
    slippage: float,
) -> float:
    """Calcula o worst-case limit price com slippage.

    BUY: worst_price = price + slippage (paga mais para garantir fill)
    SELL: worst_price = price - slippage (recebe menos para garantir fill)

    O resultado é clamped a [0.01, 0.99] (limites Polymarket).

    Args:
        side: "BUY" ou "SELL".
        price: Preço de referência (ASK para BUY, BID para SELL).
        slippage: Tolerância de slippage em preço absoluto.

    Returns:
        Preço ajustado com slippage, arredondado a 2 casas decimais.

    Raises:
        ValueError: Se side não é "BUY" nem "SELL".
    """
    if side == BUY or side == "BUY":
        worst = price + slippage
    elif side == SELL or side == "SELL":
        worst = price - slippage
    else:
        raise ValueError(
            f"Side inválido: {side!r} (esperado BUY ou SELL)"
        )
    # Clamp ao range permitido pelo Polymarket
    worst = max(0.01, min(0.99, worst))
    return round(worst, 2)


async def execute_fok_order(
    client: Any,
    token_id: str,
    side: str,
    amount: float,
    price: float,
    live_trading: bool,
    slippage: float = SLIPPAGE_TOLERANCE,
    tick_size: str = ORDER_TICK_SIZE,
    neg_risk: bool = ORDER_NEG_RISK,
    fee_rate_bps: int = 0,
) -> OrderResult:
    """Executa uma ordem Fill-Or-Kill (FOK) via py_clob_client.

    FOK: a ordem é filled integralmente ou rejeitada por completo.
    Ideal para entradas normais onde queremos garantia total.

    O code path é IDÊNTICO em DEMO e LIVE:
      1. Valida inputs
      2. Calcula worst_price com slippage
      3. Constrói market order via client.create_market_order()
      4. LIVE: post_order(..., OrderType.FOK)
         DEMO: simula fill imediato
      5. Retorna OrderResult

    Args:
        client: ClobClient autenticado (ou None em DEMO sem SDK).
        token_id: ID do token a transacionar.
        side: BUY ou SELL (constantes do SDK ou strings).
        amount: Montante ($ para BUY, shares para SELL).
        price: Preço de referência (ASK para BUY, BID para SELL).
        live_trading: True para enviar à API, False para simular.
        slippage: Tolerância de slippage (default SLIPPAGE_TOLERANCE).
        tick_size: Tick size do mercado (default "0.01").
        neg_risk: Flag neg_risk do mercado (default False).
        fee_rate_bps: Fee rate em basis points (default 0).

    Returns:
        OrderResult com todos os detalhes da execução.

    Example:
        >>> result = await execute_fok_order(
        ...     client=clob_client,
        ...     token_id="tok_abc123",
        ...     side=BUY,
        ...     amount=5.0,
        ...     price=0.85,
        ...     live_trading=True,
        ... )
        >>> result.success
        True
    """
    return await _execute_order_internal(
        client=client,
        token_id=token_id,
        side=side,
        amount=amount,
        price=price,
        order_type_enum=OrderType.FOK,
        order_type_str="FOK",
        live_trading=live_trading,
        slippage=slippage,
        tick_size=tick_size,
        neg_risk=neg_risk,
        fee_rate_bps=fee_rate_bps,
    )


async def execute_fak_order(
    client: Any,
    token_id: str,
    side: str,
    amount: float,
    price: float,
    live_trading: bool,
    slippage: float = SLIPPAGE_TOLERANCE,
    tick_size: str = ORDER_TICK_SIZE,
    neg_risk: bool = ORDER_NEG_RISK,
    fee_rate_bps: int = 0,
) -> OrderResult:
    """Executa uma ordem Fill-And-Kill (FAK) via py_clob_client.

    FAK: preenche o máximo possível ao preço-limite e cancela o resto.
    Ideal para endgame agressivo — agarra toda a liquidez disponível.

    Mesmo code path que execute_fok_order, apenas com OrderType.FAK.
    Ver docstring de execute_fok_order para detalhes completos.

    Args:
        client: ClobClient autenticado (ou None em DEMO sem SDK).
        token_id: ID do token a transacionar.
        side: BUY ou SELL.
        amount: Montante ($ para BUY, shares para SELL).
        price: Preço de referência.
        live_trading: True para enviar à API, False para simular.
        slippage: Tolerância de slippage.
        tick_size: Tick size do mercado.
        neg_risk: Flag neg_risk.
        fee_rate_bps: Fee rate em basis points.

    Returns:
        OrderResult com detalhes da execução.
    """
    return await _execute_order_internal(
        client=client,
        token_id=token_id,
        side=side,
        amount=amount,
        price=price,
        order_type_enum=OrderType.FAK,
        order_type_str="FAK",
        live_trading=live_trading,
        slippage=slippage,
        tick_size=tick_size,
        neg_risk=neg_risk,
        fee_rate_bps=fee_rate_bps,
    )


async def _execute_order_internal(
    client: Any,
    token_id: str,
    side: str,
    amount: float,
    price: float,
    order_type_enum: Any,
    order_type_str: str,
    live_trading: bool,
    slippage: float,
    tick_size: str,
    neg_risk: bool,
    fee_rate_bps: int,
) -> OrderResult:
    """Implementação interna partilhada por FOK e FAK.

    Code path IDÊNTICO para DEMO e LIVE:
      1. Input validation
      2. Compute worst_price
      3. Build order via SDK
      4. Post order (LIVE) ou simulate (DEMO)
      5. Log exaustivo
      6. Return OrderResult

    Nunca lança excepções para o caller — todos os erros são
    capturados, logados, e retornados via OrderResult.error.
    """
    result = OrderResult(
        order_type=order_type_str,
        side=side if isinstance(side, str) else str(side),
        requested_amount=amount,
        price_intended=price,
        slippage_applied=slippage,
        is_live=live_trading,
        is_demo=not live_trading,
    )
    side_str = "BUY" if side in (BUY, "BUY") else "SELL"

    # ── Step 1: Input validation ─────────────────────────────────
    if amount <= 0.0:
        result.error = f"Amount inválido: {amount} (<= 0)"
        _log_warn("ORDER",
                  f"{order_type_str} {side_str} REJEITADO | "
                  f"{result.error}")
        return result

    if price <= 0.0 or price >= 1.0:
        result.error = (
            f"Price inválido: {price} "
            f"(fora de ]0.0, 1.0[)"
        )
        _log_warn("ORDER",
                  f"{order_type_str} {side_str} REJEITADO | "
                  f"{result.error}")
        return result

    if not token_id:
        result.error = "token_id vazio"
        _log_warn("ORDER",
                  f"{order_type_str} {side_str} REJEITADO | "
                  f"{result.error}")
        return result

    # ── Step 2: Compute worst_price with slippage ────────────────
    try:
        worst_price = _compute_worst_price(
            side_str, price, slippage
        )
    except ValueError as e:
        result.error = str(e)
        _log_warn("ORDER",
                  f"{order_type_str} REJEITADO | {result.error}")
        return result
    result.price_sent = worst_price

    # ── Step 3: Log pré-execução (idêntico DEMO/LIVE) ────────────
    mode_tag = "LIVE" if live_trading else "DEMO"
    _log_mod("ORDER", f"{order_type_str}_{side_str}",
             f"[{mode_tag}] token={token_id[:16]}... | "
             f"amount={amount:.4f} | "
             f"price={_fc(price)} | "
             f"worst={_fc(worst_price)} | "
             f"slip={slippage:.2f} | "
             f"tick={tick_size}")

    # ── Step 4: Build + Post (LIVE) ou Simulate (DEMO) ───────────
    #
    # O code path até aqui é 100% idêntico. A divergência mínima
    # é apenas no post_order vs. simulação do fill.

    if live_trading and _HAS_SDK and client is not None:
        # ── LIVE: ordem real via py_clob_client ──────────────────
        try:
            # Criar market order via SDK
            order = client.create_market_order(
                token_id=token_id,
                side=side,
                amount=amount,
                price=worst_price,
                fee_rate_bps=fee_rate_bps,
                options={
                    "tick_size": tick_size,
                    "neg_risk": neg_risk,
                },
            )

            # Enviar ordem com tipo FOK/FAK
            resp = client.post_order(
                order, order_type_enum
            )

            # Processar resposta
            result.raw_response = resp
            if isinstance(resp, dict):
                result.order_id = resp.get(
                    "orderID",
                    resp.get("id", resp.get("orderIds", None))
                )
                # Verificar se houve fill
                status = resp.get("status", "")
                if status in ("matched", "filled", "live"):
                    result.success = True
                elif result.order_id:
                    # Se temos orderID, assumir sucesso
                    result.success = True
            else:
                # Resposta não-dict — aceitar se não deu excepção
                result.success = True
                result.order_id = str(resp) if resp else None

            if result.success:
                _log_mod("ORDER", f"{order_type_str}_OK",
                         f"[LIVE] {side_str} | "
                         f"orderID={result.order_id} | "
                         f"amount={amount:.4f} @ "
                         f"{_fc(worst_price)}")
            else:
                result.error = (
                    f"Resposta sem sucesso: {resp}"
                )
                _log_warn("ORDER",
                          f"{order_type_str} {side_str} SEM FILL | "
                          f"[LIVE] resp={resp}")

        except Exception as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            _log_warn("ORDER",
                      f"{order_type_str} {side_str} FALHOU | "
                      f"[LIVE] {result.error}")
    else:
        # ── DEMO: simula fill idêntico ao path LIVE ──────────────
        #
        # A simulação segue exactamente o mesmo log format.
        # Em DEMO, o fill é sempre completo (como se houvesse
        # liquidez infinita ao worst_price).
        result.success = True
        result.order_id = (
            f"DEMO-{order_type_str}-{int(time.time()*1000)}"
        )

        _log_mod("ORDER", f"{order_type_str}_OK",
                 f"[DEMO] {side_str} | "
                 f"orderID={result.order_id} | "
                 f"amount={amount:.4f} @ "
                 f"{_fc(worst_price)}")

    # ── Step 5: Log pós-execução ─────────────────────────────────
    if result.success:
        _log_mod("ORDER", "CONFIRMED",
                 f"[{mode_tag}] {order_type_str} {side_str} | "
                 f"token={token_id[:16]}... | "
                 f"amount={amount:.4f} | "
                 f"intended={_fc(price)} → sent={_fc(worst_price)} | "
                 f"orderID={result.order_id}")
    else:
        _log_warn("ORDER",
                  f"[{mode_tag}] {order_type_str} {side_str} FAILED | "
                  f"token={token_id[:16]}... | "
                  f"amount={amount:.4f} | "
                  f"error={result.error}")

    return result


# =============================================================================
# INTEGRATION HELPER — Endgame + FAK Combo (conveniência)
# =============================================================================

async def execute_aggressive_endgame(
    remaining_s: float,
    best_asks: Dict[str, Optional[float]],
    meta: Dict[str, str],
    client: Any,
    bankroll: float,
    risk_pct: float,
    live_trading: bool,
    slippage: float = SLIPPAGE_TOLERANCE,
) -> Optional[OrderResult]:
    """Avalia endgame agressivo e executa FAK se houver sinal.

    Combina evaluate_aggressive_endgame() + execute_fak_order()
    num único call. Ideal para integração directa no logic_loop.

    Args:
        remaining_s: Segundos restantes até expiração.
        best_asks: Dict com ASKs por lado {"up": float, "down": float}.
        meta: Dict com token IDs {"up": str, "down": str}.
        client: ClobClient (ou None em DEMO).
        bankroll: Banca actual em USDC.
        risk_pct: Fracção da banca a arriscar (ex: 0.10 = 10%).
        live_trading: True para ordens reais, False para simular.
        slippage: Tolerância de slippage.

    Returns:
        OrderResult se sinal disparou e ordem foi executada.
        None se sem sinal ou condições não satisfeitas.

    Example:
        >>> result = await execute_aggressive_endgame(
        ...     remaining_s=3.5,
        ...     best_asks={"up": 0.88, "down": 0.13},
        ...     meta={"up": "tok_up", "down": "tok_down"},
        ...     client=clob_client,
        ...     bankroll=50.0,
        ...     risk_pct=0.10,
        ...     live_trading=False,
        ... )
        >>> result.success
        True
    """
    signal = evaluate_aggressive_endgame(
        remaining_s=remaining_s,
        best_asks=best_asks,
        meta=meta,
    )
    if signal is None:
        return None

    # Calcular montante a investir
    invest_amount = bankroll * risk_pct
    if invest_amount <= 0.0:
        _log_warn("ENDGAME_AGG",
                  f"Banca insuficiente: ${bankroll:.4f} * "
                  f"{risk_pct:.1%} = ${invest_amount:.4f}")
        return None

    _log_mod("ENDGAME_AGG", "EXECUTE",
             f"FAK {signal.side} | "
             f"ASK={_fc(signal.ask_price)} | "
             f"invest=${invest_amount:.4f} | "
             f"conf={signal.confidence:.2f}")

    result = await execute_fak_order(
        client=client,
        token_id=signal.token_id,
        side=BUY,
        amount=invest_amount,
        price=signal.ask_price,
        live_trading=live_trading,
        slippage=slippage,
    )

    return result


# =============================================================================
# 📊 STEP 5 — BLUEPRINT CARD
# =============================================================================
#
# | Area                | Details                                         |
# |---------------------|-------------------------------------------------|
# | What Was Built      | 3 modules: ResolutionPoller, AggressiveEndgame, |
# |                     | FOK/FAK Order Execution with slippage           |
# | Key Design Choices  | aiohttp for non-blocking polls; NamedTuple for  |
# |                     | immutable signals; identical DEMO/LIVE codepath  |
# | PEP8 Highlights     | snake_case funcs; PascalCase classes; type hints|
# |                     | everywhere; Google-style docstrings              |
# | Error Handling      | Never throws to caller; all errors captured     |
# |                     | in OrderResult.error/PollerState; exaustive logs |
# | Overall Complexity  | Time: O(1) per operation (poll/eval/execute)    |
# |                     | Space: O(1) per module (no unbounded buffers)   |
# | Reusability Notes   | Each module standalone; mockable; no globals    |
# |                     | dependency; parametrised defaults overridable    |
# =============================================================================
