"""Valor justo + sinal direcional (LMSR + Bayesiano, portados dos bots XRP do
repo) traduzidos para LP farming.

A traducao NAO e a obvia. No repo o sinal enviesava as cotacoes de um market maker
direcional: aperta o lado que queres executar, alarga o outro. Isso funciona
quando cada perna ganha por si. Em LP farming na banda extrema o score e
`Q_min = min(Q_bid, Q_ask)` -- a perna PIOR fixa a pontuacao -- e entao qualquer
assimetria e perda pura: pagas o reward da perna larga e nao recebes nada pela
apertada. Medido com o scoring desta ferramenta (D=2c, mid 4.6c):

    vies   d_bid   d_ask   reward retido
    0.10   0.00    0.35        68%
    0.30   0.00    1.05        23%
    0.50   0.00    1.75         1.6%

Ou seja: o skew assimetrico "preditivo" troca ~98% do reward por esquivar fills
num lado. Coberto pelo teste `test_delta_assimetrico_destroi_qmin`.

O que se faz em vez disso: a toxicidade prevista entra como MULTIPLICADOR DO CUSTO
esperado por fill, e o optimizer re-resolve o delta* -- simetrico, dentro da band,
com a mesma logica de regime. Mais toxicidade => custo maior => delta* recua (ou o
mercado passa a BORDA e deixa de se farmar). O sinal informa o preco do risco; nao
substitui o optimizer.

Estado: NAO VALIDADO. No harness `paper` o mid e um martingale puro
(`fillmodel.step_mid` com drift=0) e o sinal e alimentado pelo proprio mid, por
isso nao pode ter poder preditivo por construcao -- so se pode observar a operar,
nunca a acertar. Precisa de um feed do subjacente (Binance WS para cripto, oraculo
para desportos) e de um backtest com PnL antes de valer alguma coisa. Por isso o
ganho vem a 0.0 (desligado) por omissao.
"""
from __future__ import annotations
import math


class LMSRPricer:
    """Logarithmic Market Scoring Rule. p_up = sigmoid((q_up - q_down)/b).
    b maior => spread mais apertado."""

    def __init__(self, b: float = 100_000.0):
        self.b = b
        self._q_up = 0.0
        self._q_down = 0.0

    def update_quantities(self, q_up: float, q_down: float) -> None:
        self._q_up, self._q_down = q_up, q_down

    def price_up(self) -> float:
        diff = max(-20.0, min(20.0, (self._q_up - self._q_down) / self.b))
        return 1.0 / (1.0 + math.exp(-diff))

    def inefficiency(self, market_price_up: float) -> float:
        return self.price_up() - market_price_up


class BayesianSignal:
    """Update Bayesiano sequencial em log-odds: cada retorno do subjacente empurra
    a crenca P(subida).

    Duas adaptacoes face ao original do repo, que corria em XRP 5-min:
      - esquecimento (`decay` < 1). O original reinicia a cada mercado de 5 min;
        num mercado continuo sem reset as log-odds saturam.
      - winsorizacao do incremento (`max_step`). `ret/var` explode fora da gama de
        retornos minusculos do XRP a 5 minutos.
    """

    def __init__(self, prior: float = 0.50, likelihood_std: float = 0.15,
                 decay: float = 0.90, max_step: float = 2.0):
        self.var = max(1e-6, likelihood_std ** 2)
        self.decay = decay
        self.max_step = max_step
        self._log_odds = math.log(prior / (1.0 - prior)) if 0 < prior < 1 else 0.0

    @property
    def p_up(self) -> float:
        return 1.0 / (1.0 + math.exp(-max(-20.0, min(20.0, self._log_odds))))

    def update(self, ret: float) -> None:
        step = max(-self.max_step, min(self.max_step, ret / self.var))
        self._log_odds = self.decay * self._log_odds + step

    def reset(self, prior: float = 0.50) -> None:
        self._log_odds = math.log(prior / (1.0 - prior)) if 0 < prior < 1 else 0.0


class ToxicitySignal:
    """Vies de deriva do mid -> multiplicador do custo esperado por fill.

    `gain` = 0.0 desliga (default). `gain` = 1.0 significa "no vies maximo, contar
    o dobro do custo por fill", o que empurra o delta* para tras de forma simetrica
    e, em mercados marginais, atira o mercado para BORDA -- que e a decisao certa
    quando se preve fluxo informado.
    """

    def __init__(self, lmsr_b=100_000.0, prior=0.50, likelihood_std=0.15,
                 lean_min=0.03, gain=0.0, w_lmsr=0.0):
        self.lmsr = LMSRPricer(lmsr_b)
        self.bayes = BayesianSignal(prior, likelihood_std, decay=0.90, max_step=2.0)
        self.lean_min = lean_min      # abaixo disto o sinal e ruido, ignora-se
        self.gain = gain              # 0 = desligado
        self.w_lmsr = w_lmsr          # >0 so com quantidades LMSR reais
        self._last_px = None

    def on_underlying(self, price: float) -> None:
        if self._last_px is not None and self._last_px > 0:
            self.bayes.update((price - self._last_px) / self._last_px)
        self._last_px = price

    def lean(self, market_mid_up: float = 0.5) -> float:
        """Vies de deriva em [-0.5, 0.5]: >0 subida esperada, ~0 em lateral."""
        drift = self.bayes.p_up - 0.5
        struct = self.w_lmsr * self.lmsr.inefficiency(market_mid_up) if self.w_lmsr > 0 else 0.0
        return max(-0.5, min(0.5, drift + struct))

    def cost_multiplier(self, market_mid_up: float = 0.5) -> float:
        """Multiplicador (>= 1) do custo esperado por fill. Simetrico por desenho:
        sob `Q_min = min(...)` a assimetria e perda pura (ver o topo do modulo)."""
        if self.gain <= 0.0:
            return 1.0
        lean = abs(self.lean(market_mid_up))
        if lean < self.lean_min:
            return 1.0
        return 1.0 + self.gain * (lean - self.lean_min) / (0.5 - self.lean_min)
