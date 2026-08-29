# Prompt Master — LP BOOK PRO

**Estado:** especificação, agora com uma implementação de referência em `lpbook/` (13 ficheiros,
1089 linhas). Os testes das partes puras passam e o modo `paper` corre de ponta a ponta; `scan` e
`live` precisam da rede do Polymarket e não foram exercitados.
**Verificado contra o repo em:** 29/08/2026 (commit `9d2bc33`).
**Revisto contra a implementação em:** 29/08/2026 — a tese "existe sempre δ\* interior" **caiu**
(secção 0.5), e a calibração de `k` tem um problema de identificação por resolver (secção 0.6).
**Como usar:** colar as secções 1–11 como instrução numa sessão nova. Ler primeiro a secção 0 —
o prompt original citava infraestrutura deste repo que não existe, e as correções estão lá.

Público: um único operador técnico (Python, asyncio, APIs Polymarket CLOB/Gamma já conhecidas).
Código conciso e minimal, sem placeholders, sem variáveis abreviadas, comentários e output em PT-PT.

---

## 0. Verificação contra o repo

O prompt original diz, na secção 6, para "reaproveitar os padrões já existentes (bots XRP)".
Metade desses padrões não existe neste repo. Esta secção é o mapa real, para a sessão de
construção não perder tempo à procura de ficheiros que não estão cá.

### 0.1 Existe e é reaproveitável — âncoras reais

| O que o prompt pede | Onde está de facto |
|---|---|
| jump-diffusion (§3.4) | `compute_jump_diffusion_probability()` em `xrp_bot_v9_4_1.py:1637` — mistura de Poisson à Merton, `n_terms` truncado. Parâmetros em `xrp_bot_v9_4_1.py:224-227` (`jump_lambda`, `jump_mu`, `jump_sigma`, `jump_terms`) |
| estado em JSON durável (§6, `state.py`) | `TradeStateManager` em `xrp_bot_v9_4_1.py:1039` — escreve `.tmp`, roda o original para `.bak`, `rename` para o canónico; `save_async()` despacha o bloqueante para executor |
| modelo de fill do modo `paper` (§6, §9.5) | `ShadowFillEngine` em `xrp_bot_v9_4_1.py:820` — simula fills contra o livro L2 **ao vivo**: snapshot pré-latência, espera de latência, releitura do livro, VWAP sobre a profundidade real, rejeição por slippage. É exatamente o modelo de fill pedido, já escrito |
| endpoints Gamma / CLOB / WS (§4) | constantes em `xrp_bot_v9_4_1.py:206-209`: `clob.polymarket.com`, `gamma-api.polymarket.com`, `wss://ws-subscriptions-clob.polymarket.com/ws/market` e `/ws/user`. Duplicadas em `notifications.py:291-294` e `xrp_true_market_maker_v5_3_1.py:151-152` |
| credenciais | `_load_secrets_file()` em `xrp_bot_v9_4_1.py:94` — lê `secrets.txt` em `chave=valor` e avisa se as permissões do ficheiro forem laxas |
| cliente CLOB do modo `live` (§6, §9.6) | `py_clob_client` importado com fallback em `xrp_true_market_maker_v5_3_1.py:42-44` (`ClobClient`, `OrderArgs`, `OrderType`, `ApiCreds`, flag `_HAS_CLOB`) |

Correção a aplicar ao reaproveitar o estado: `TradeStateManager._save_blocking()` roda o ficheiro
canónico para `.bak` **antes** de promover o `.tmp`, o que abre uma janela em que o caminho
canónico não existe. Um leitor externo (telemetria) que abra nesse instante apanha `FileNotFoundError`.
No `state.py` novo, usar `os.replace(tmp, canónico)` direto — substituição atómica sem janela — e
escrever o `.bak` a partir do conteúdo anterior já lido, se for preciso histórico.

### 0.2 Não existe — é construção nova, não reaproveitamento

- **`control.json` / kill-switch: zero ocorrências** nos três ficheiros. O padrão mais próximo é
  `config_hot_reload_loop()` em `xrp_true_market_maker_v5_3_1.py:195` (poll de `mtime` a cada
  `config_reload_interval_s`, aplica um allowlist de campos em `_HOT_RELOAD_FIELDS`), mais as
  flags `dry_run` / `live_trading` / `shadow_mode` lidas do ambiente em
  `xrp_true_market_maker_v5_3_1.py:190-192`. O `control.json` deve ser **modelado nesse loop**,
  mas é escrito de raiz.
- **Painel de telemetria Streamlit: não existe.** Zero ocorrências de `streamlit`. O `state.json`
  novo não tem hoje nenhum consumidor — se o painel é desejado, é entregável à parte.
- **Deploy systemd: não existe.** Zero ocorrências. Nenhum unit file no repo.
- **TUI: não existe base nenhuma.** Nenhum ficheiro importa `rich`, `textual` ou `curses`. A
  secção 8 é 100% nova e `rich` é dependência nova (o repo hoje pede `websockets`,
  `py-clob-client`, `web3`, `requests`, `orjson`, `numpy` — `README.md` §7).
- **Nada de rewards.** Zero ocorrências de `rewards`, `hawkes`, `avellaneda`, `reservation`. Toda
  a mecânica das secções 3–5 é nova.

### 0.3 Armadilha de nomes

`max_spread_cents` (`xrp_bot_v9_4_1.py:160`, default `1.45`) é um **filtro de spread do livro**
para rejeitar entradas em mercados largos, usado em `xrp_bot_v9_4_1.py:4070`. Não tem relação
nenhuma com o `rewards_max_spread` da secção 4, que é a largura da banda de reward lida do Gamma.
Não reutilizar o nome nem o valor.

### 0.4 Correção ao solver de δ\* (a mais importante)

A secção 3.2 do prompt original manda "bissecção ou Newton em `[0, D]`" e afirma "solução interior
única". **Falha como está escrito.** Com

```
U(delta)   = R * ((D - delta) / D)^2  -  C * A * exp(-k * delta)
U'(delta)  = -2R (D - delta) / D^2  +  C k A exp(-k delta)
U''(delta) =  2R / D^2  -  C k^2 A exp(-k delta)
```

repare-se que `U'(D) = C k A exp(-k D) > 0` para **quaisquer** parâmetros. Quando a condição de
solução interior `C k A > 2R/D` se cumpre, `U'(0) > 0` **e** `U'(D) > 0`: o sinal é o mesmo nos
dois extremos e a bissecção em `[0, D]` não tem o que bracketar. Pior: se `C k A < 2R/D`, existe
mudança de sinal em `[0, D]`, mas a raiz que a bissecção encontra é um **mínimo** de `U`, não o
ótimo — colocar aí é o pior sítio da banda.

A estrutura real: `U` é côncava e depois convexa, com a inflexão em

```
delta_m = (1/k) * ln( C * k^2 * A * D^2 / (2R) )      # onde U'' = 0
```

`U'` desce até `delta_m` e sobe depois. Logo há no máximo duas raízes: a primeira (em `[0, delta_m]`)
é o máximo procurado, a segunda é um mínimo. Algoritmo correto:

1. calcular `delta_m`;
2. se `delta_m <= 0` → `U` convexa em toda a banda → ótimo num canto: comparar `U(0)` e `U(D)`;
3. se `delta_m >= D` → `U'` positiva em toda a banda → `U` crescente → ótimo em `D`;
4. caso contrário, se `U'(delta_m) < 0` → bissectar `U'` em `[0, delta_m]` → é o `delta*` interior;
   se `U'(delta_m) >= 0` → `U` crescente → ótimo em `D`;
5. **`delta* = argmax U` sobre `{0, raiz interior, D}`** — nunca aceitar a raiz sem comparar cantos;
6. varrimento em grelha fina sobre `[0, D]` como rede de segurança e como teste de regressão do solver.

Duas leituras que caem de graça deste tratamento:

- `U(D) = -C*A*exp(-k*D) < 0` sempre. Portanto o canto `D` só vence quando **todo** `delta` dá
  utilidade negativa — e isso é o critério de rejeição do mercado da secção 5 (`E[líquido] <= 0`),
  obtido pela mesma conta. O solver e o selector partilham o mesmo veredito.
- O canto `delta = 0` só vence quando `C k A <= 2R/D`, que é exatamente a prova por mercado que a
  secção 7 exige antes de deixar cotar no mid.

A condição `C k A > 2R/D` da secção 3.2 está **correta** tal como escrita; o que estava errado era
só o método de a resolver.

Nota de honestidade sobre `R`: `R` já vem escalado pela share esperada da pool, e a share depende
de `delta` (o próprio score entra no denominador da normalização). Tratar `R` como constante é uma
aproximação de primeira ordem válida enquanto a nossa fatia for pequena face ao total dos makers.
Re-resolver `delta*` a cada requote, com o `R` do estado competitivo observado nessa amostra, e
não uma vez por sessão.

> **Superado pela implementação:** esta análise fechada vale para o objetivo tal como escrito na
> secção 3.1. A implementação substituiu esse objetivo (share explícita + fator de permanência na
> band — ver 0.5), e a forma resultante já não é analítica. `lpbook/optimizer.py` resolve por grelha
> de 240 pontos mais refinamento por secção áurea, o que é a escolha certa para uma forma
> desconhecida. Mantém-se aqui porque a conclusão estrutural — **o ótimo pode ser um canto, e a
> bissecção ingénua encontra o mínimo** — foi o que a implementação confirmou.

### 0.5 Os três regimes (a tese corrigida)

O objetivo da secção 3.1 é incompleto: sem penalizar a borda, o ótimo degenera para lá em mercados
finos (a receita achata mas o custo continua a cair). Falta o termo que o próprio material de origem
descreve — perto da borda, o **drift do mid empurra a perna para fora da band e passa a marcar
zero**. O objetivo completo, que é o que `lpbook/optimizer.py` implementa:

```
s(d)      = size * ((D - d)/D)^2                    # score quadratico
g(d)      = 2*Phi((D - d)/sd) - 1                   # P(fica na band apesar do drift)
reward(d) = pool_ps * s(d)/(s(d) + q_others) * g(d) # share EXPLICITA, $/s
cost(d)   = c_loss * A * exp(-k*d)                  # $/s
U(d)      = reward(d) - cost(d)
```

Duas coisas mudam face à secção 3.1: a share entra explicitamente (`s/(s+q_others)`, saturante — é
o que faz o size ter retornos decrescentes) e `g(d)` penaliza a borda. Com isto, o ótimo cai num de
**três regimes**, e não num "interior sempre positivo":

| regime | onde | quando |
|---|---|---|
| **MID** (`δ* ≈ 0`) | em cima do mid | custo de fill desprezável e pool rico — o mid é mesmo o melhor |
| **INTERIOR** (`0 < δ* < D`) | recuo intermédio | `k` alto: os fills concentram-se no mid, recuar um cabelo evita a toxicidade sem perder share |
| **BORDA** (`δ* ≈ D`) | encostado à borda | fino ou tóxico — na prática **não farmar**; o `rho` rejeita |

Varrimento que corri para localizar as fronteiras (`D=2c`, `size=200`, `q_others=3000`, `A=0.02`,
`sd=0.6c`; `U*` anualizado a $/dia):

| pool $/d | k | c_loss | δ\* | regime | U\* |
|---|---|---|---|---|---|
| 5000 | 0.9 | 0.05 | 0.000c | MID | +225.83 |
| 5000 | 0.9 | 1.8 | 2.000c | BORDA | −514.15 |
| 5000 | 6.0 | 1.8 | 0.748c | INTERIOR | +87.63 |
| 480 | 0.9 | 0.05 | 2.000c | BORDA | −14.28 |
| 480 | 3.0 | 0.05 | 0.882c | INTERIOR | +3.06 |
| 480 | 6.0 | 1.8 | 1.215c | INTERIOR | +1.83 |
| 21.6 | 3.0 | 0.05 | 2.000c | BORDA | −0.21 |
| 21.6 | 6.0 | 0.05 | 1.110c | INTERIOR | +0.13 |
| 21.6 | 6.0 | 1.8 | 2.000c | BORDA | −0.02 |

Leitura honesta destes números — **não é "o k decide tudo"**:

- **`k` decide se o INTERIOR existe.** Com `k = 0.9` não há um único interior no varrimento: só MID
  ou BORDA. Com `k = 6` (fills muito concentrados no mid) o interior aparece em quase todo o lado.
  É por isto que calibrar o `k` é a joia da coroa e não um rodapé: é o parâmetro que diz se existe
  sequer um sítio inteligente onde pousar.
- **A razão reward/custo decide MID vs BORDA.** Com o mesmo `k = 0.9`, pool rico e custo baixo dá
  MID; pool fino ou custo alto dá BORDA. O `k` não distingue estes dois.
- **BORDA implica sempre prejuízo, por construção.** Em `δ = D` o score é zero **e** `g(D) = 0`,
  logo `U(D) = −c_loss·A·e^(−kD) < 0` sempre. "O ótimo é a borda" é sinónimo de "este mercado não se
  farma" — o mesmo veredito que o `rho` dá, por outro caminho. Isto é teorema, não acidente do
  varrimento.
- O rótulo MID/INTERIOR usa um corte em `0.05·D`: é uma etiqueta sobre um contínuo, não uma
  descontinuidade física. O caso `δ* = 0.100c` acima está no limiar.

### 0.6 O que a verificação da implementação encontrou

Corri os testes (8/8 passam) e o modo `paper` de ponta a ponta. Confirmei os números reportados:
a $20 de bankroll o scan rejeita 4 dos 5 mercados sintéticos e arma o qualifier de ténis de baixa
competição (pool $12/dia, δ\* MID, net +$2.89/dia, `rho` 0.52). Três achados que não estavam
assinalados:

1. **A calibração de `k` não arrancava — problema de identificação. ✅ CORRIGIDO** (ver 0.7 para o
   que foi preciso e o que custou). O diagnóstico: o bucket era indexado por
   `round(self.st.delta_c, 1)`, o δ **corrente**. Como a política converge para um único δ, todos os
   fills caíam num só bucket, a regressão ficava com `sxx = 0` e `calibrate_ak` devolvia `None` para
   sempre — numa corrida `paper` de 2 h com 7 fills os priores saíram intactos. **Não se estima o
   declive de `λ(δ)` com fills recolhidos todos ao mesmo δ.**
2. **Sem WebSocket e sem asyncio.** `data_feed.py` é REST síncrono (`httpx`), tanto para o Gamma
   como para o book (`/book?token_id=`). A secção 6 pede CLOB WS para book/mid ao vivo, e o resto do
   repo é todo asyncio. Com polling REST, o uptime — que a secção 4 diz pontuar diretamente — fica
   refém do intervalo de poll. `LiveExecutor.poll_fills()` devolve `[]` com um comentário a dizer
   que é preciso ligar o WS de user: em `live`, hoje, o inventário, o skew e a calibração **não
   recebem fills nenhuns**.
3. **Sem contabilidade de order/cancel budget.** Não há nada no código a contar ordens ou
   cancelamentos, e `LiveExecutor.place()` faz `cancel_all()` a cada requote. As secções 4 e 7 exigem
   dimensionar o loop ao tier do signer. É o tipo de teto que se descobre numa sessão volátil, que é
   exatamente o que a secção 4 avisa para não fazer.

Nota de dependências: `lpbook/` traz `httpx` e `rich`, nenhuma delas usada pelos bots XRP (que usam
`requests` e `websockets`). Ver `lpbook/README.md`.

### 0.7 Correção da calibração — o que foi preciso, e o que custou

O achado 1 está corrigido. **A correção que eu próprio recomendei como suficiente não era.**
Registo a sequência porque o erro é instrutivo:

**Passo 1 — indexar o bucket por perna** (`book_engine.py`, `observe_time`/`on_fill`, com o executor
a devolver a distância que gerou o fill). Resultado: a calibração passou a *correr* — mas devolveu
`k = 0.000` contra um `k` verdadeiro de 0.9. Porquê: no regime MID (`δ* = 0`) as duas pernas ficam
ambas à distância do skew, ou seja **na mesma distância**; a única variação em δ vinha da deriva do
inventário, uma janela estreita e endógena. O mecanismo destrancou; a identificação não.

**Passo 2 — estimador que usa os buckets vazios.** A regressão `ln(λ)` vs δ precisa de `ln(f/s)` e
por isso descarta todo o bucket com zero fills — mas *"zero fills em 5000 s a 1.4c"* é precisamente
a observação que fixa o `k`. Descartá-los deixa só os buckets com sorte e enviesa o `k` para baixo.
Substituído por MLE de Poisson (`λ(d) = A·e^(−kd)`, `A` em forma fechada dado `k`, procura 1-D em
`k`), que usa toda a exposição. Dois guardas de honestidade: se o ótimo foge para `k_max`, é porque
todos os fills caíram na menor distância exposta — ausência de informação, não decaimento infinito;
devolve `None`. E exige amplitude mínima entre distâncias (`min_span_c`): estimar um decaimento numa
janela de 0.1c e extrapolá-lo para uma band de 2c é adivinhar com aritmética.

**Passo 3 — dithering de δ.** Sem variação imposta não há declive. Ciclo simétrico de 4 fases à volta
do δ\* (`--dither-frac`, default 0.30 da band); o regime continua a ser classificado no δ\*
verdadeiro, não no dithered.

Medição, 5 seeds × 72 h simuladas, `k` verdadeiro = 0.9:

| seed | sem dithering | com dithering (0.30) |
|---|---|---|
| 0 | 0.578 | 1.227 |
| 100 | **0.000** | 0.868 |
| 200 | 1.435 | 0.877 |
| 300 | 0.900 | 1.411 |
| 400 | 0.492 | 1.040 |
| **erro absoluto médio** | **0.433** | **0.207** |
| **líquido médio** | **$24.60** | **$22.46** |

Leitura honesta, com n=5: o erro médio cai para metade, mas **3 dos 5 seeds melhoram e 2 pioram
ligeiramente** — a média cai sobretudo por eliminar o falhanço total (o `k = 0.000` do seed 100, que
teria mandado o regime para o sítio errado com confiança). O dithering custou **8.7% do líquido**
nesta configuração: é o preço explícito da informação, e é a troca certa quando é o `k` que decide o
regime. Amplitudes de 0.15 e 0.30 são indistinguíveis nestes dados (erro médio 0.213 vs 0.207) — o
valor exato quer um varrimento a sério antes de ser afinado; o default de 0.30 não está otimizado.

**Achado adicional, encontrado pelo teste da correção:** com o skew ativo, as pernas ficavam em
`r ± d` à volta do mid enviesado e a perna do lado contrário ao inventário **saía da band** (2.4c
numa band de 2.0c, com o inventário no cap). Fora da band a perna marca zero e, na banda extrema
(mid < 10c), `Q_min = min(dois lados) = 0` — **o par inteiro deixa de pontuar por causa de uma
perna**. Corrigido com clamp de ambas as pernas à band: o skew gasta-se do orçamento da band, não
além dele. Coberto por teste.

### 0.8 Fusão com os bots XRP do repo — o que entrou e o que foi rejeitado

Uma sessão paralela portou quatro peças dos bots XRP para o `lpbook/`: fees/rebate reais, LMSR +
sinal Bayesiano traduzidos em **δ assimétrico por perna**, shadow fill contra livro L2, e
config/secrets. Verifiquei cada uma. Três entraram, com alterações; **a peça apresentada como a
síntese central foi rejeitada, por aritmética.**

**Rejeitado — δ assimétrico por perna.** A ideia: se o valor justo diz que o mid vai subir, um fill
no teu ask é tóxico e no bid é benigno; logo aperta o bid e alarga o ask. Isto é **correto para um
market maker direcional** — que é o uso original no repo, onde cada perna ganha por si. É **errado
para LP farming**, porque na banda extrema o score é `Q_min = min(Q_bid, Q_ask)`: a perna **pior**
fixa a pontuação, e a perna apertada não conta para nada. Medido com o `scoring.py` da própria
ferramenta (`D = 2c`, mid 4.6c, `gain_c = 3.5` do port):

| viés | d_bid | d_ask | reward retido |
|---|---|---|---|
| 0.10 | 0.00c | 0.35c | 68% |
| 0.30 | 0.00c | 1.05c | 23% |
| 0.50 | 0.00c | 1.75c | **1.6%** |

A frame que a sessão apresentou como prova de funcionamento — `vies +0.50 (ask toxica) d_bid 0.00c
d_ask 1.74c` — é o caso da última linha: **o reward a cair para 1.6%** para esquivar fills de um
lado. Com `base δ ≥ 0.5c` e viés ≥ 0.3 o ask sai da band e o `Q_min` vai a **zero**. Sob `min()`,
qualquer assimetria é perda pura: pagas o reward da perna larga e não recebes nada pela apertada.
Registado em `test_delta_assimetrico_destroi_qmin`, para não voltar.

**O que entrou no lugar:** a toxicidade prevista entra como **multiplicador do custo esperado por
fill**, e o `optimizer` re-resolve o δ\* — simétrico, dentro da band, com a mesma lógica de regime.
Mais toxicidade → custo maior → δ\* recua, ou o mercado passa a BORDA e deixa de se farmar. O sinal
passa a informar o *preço* do risco em vez de substituir o optimizer.

**Estado do sinal: NÃO VALIDADO, e desligado por omissão (`--signal-gain 0`).** No harness `paper` o
mid é um martingale puro (`fillmodel.step_mid` com `drift = 0`) e o sinal é alimentado pelo próprio
mid — **não pode ter poder preditivo por construção**, só se pode observar a operar. A/B em 3 seeds
× 72 h: líquido médio $21.02 (desligado) vs $17.60 (ligado) — pior, que é o que a teoria prevê
quando se paga por informação que não existe. Precisa de um feed do subjacente (Binance WS para
cripto, oráculo para desporto) e de um backtest com PnL antes de valer alguma coisa.

**Entrou com alteração — fees.** A `taker_fee` é real e útil: escoar inventário a mercado cruza o
spread, e é o único ponto do farming que paga taker ($0.022 por 1000 shares a 4.6c). Ligada ao
caminho de flatten, que antes era contabilizado como grátis. **O rebate de maker ficou a 0 bps por
omissão**, por três razões: (a) os 20 bps vêm do default `maker_rebate_bps` do
`xrp_true_market_maker_v5_3_1.py` — configuração de um bot, não documentação do Polymarket, e não
verificável aqui; (b) ligá-lo faz o fill parecer parcialmente receita, que é o que a §10 proíbe;
(c) mesmo a 20 bps vale **0.5–1.3% do movimento adverso** ($0.000092/share contra $0.012/share a
4.6c) — não muda decisão nenhuma, só torna as projeções mais otimistas. Confirmado na doc oficial,
passa-se `bps=20` explicitamente.

**Entrou — shadow fill.** Modelo de fill contra livro L2 com latência, slippage e fills parciais.
Nota: na entrega original **não estava ligado a lado nenhum** (zero importadores) — o modo `paper`
continuava a usar o `try_fill` sintético. Fica como módulo testado, disponível para o modo online.

**Não entrou — `config.py`.** Duplica o `_load_secrets_file()` que já existe em
`xrp_bot_v9_4_1.py:94` e, na entrega original, também não tinha consumidor nenhum. O `LiveExecutor`
recebe um cliente CLOB já autenticado, portanto não há o que configurar.

**Nota de integração importante:** a fusão foi construída sobre a versão do `lpbook/` **anterior** às
correções da secção 0.7 — aplicá-la tal como veio revertia o bucket por perna, o dithering e o clamp
da band. As peças foram portadas por cima da versão corrigida, não o contrário.

**Corrigido no port:** o ficheiro chamava-se `signal.py`, que **sombra o módulo `signal` da
biblioteca padrão** — uma armadilha latente para qualquer coisa que precise de tratar sinais.
Renomeado para `toxicity.py`.

### 0.9 Os dois bloqueios do `live` — orçamento de ordens e caminho de fills

Ficavam por resolver os dois itens da §0.6 que impedem o `live` de ser real.

**Orçamento de order/cancel (§4, §7).** Um requote custa 2 cancelamentos + 2 ordens contra o tier do
signer. O dithering da §0.7 tornou isto mais urgente, não menos: cada ciclo passa a mexer mesmo as
pernas, portanto gasta sempre. `budget.py` implementa uma **janela deslizante real** (deque de
timestamps, não um contador que reinicia — um contador por minuto de relógio deixa passar o dobro do
limite na fronteira). O `requote` consulta-a e degrada por camadas:

1. movimento abaixo do tick → não repõe (churn puro: gasta orçamento, arrisca a fila, não muda o
   score de forma mensurável);
2. sem orçamento → **mantém as pernas antigas**. Ficar de pé com uma cotação um pouco velha pontua;
   ficar sem pernas não pontua nada (o uptime pontua diretamente, §4). O pior caso — cancelar e
   depois não conseguir repor — fica excluído por construção;
3. o **dithering cede ao orçamento**: sem folga, cota-se no δ\* e não se recolhe informação neste
   ciclo.
4. a retirada por burst (§3.5) **ignora** o orçamento: é ação de risco, não de otimização.

Medido em 24 h simuladas (o loop pede ~3 requotes/min):

| ordens/min | requotes travados | líquido |
|---|---|---|
| 30 (folgado) | 0 | $5.34 |
| 4 | 1285 | $5.08 |
| 2 | 2729 | $4.91 |

Degrada suavemente (−5% e −8%) em vez de partir. **Os limites reais por tier do Polymarket não estão
codificados** — não foi possível confirmá-los — por isso o default é deliberadamente conservador
(30/min) e os números passam-se em `--orders-per-min` / `--cancels-per-min`. A ferramenta nunca deve
ser a primeira a descobrir o teto.

**Caminho de fills em `live` — e uma assimetria séria entre paper e live.** No `paper` o gerador
devolve o movimento adverso **no mesmo instante** do fill. Em live isso é impossível: no momento em
que enches só sabes o preço a que encheste; o movimento adverso é o que o mid faz **a seguir** — é a
própria definição de seleção adversa. Ligar o WS de user de forma ingénua (adverse = 0 no instante
do fill) faria a ferramenta concluir que **os fills são grátis**, que é exatamente o erro que ela
existe para evitar, e o `rho` deixaria de rejeitar seja o que for.

`live_fills.py` resolve isso com uma fila: o fill entra pendente, e só é liquidado depois de um
horizonte, com o deslocamento do mid medido. Decisões que valem a pena registar:

- **O adverso é sinalizado**, não clampado a zero: um fill que correu bem entra negativo e baixa o
  custo medido. Clampar enviesava o custo para cima e fazia rejeitar mercados bons.
- **Mas o `c_loss` entregue ao optimizer nunca é negativo** — um custo negativo inverteria o termo e
  o optimizer passaria a *querer* fills, o oposto de tudo o que a ferramenta faz.
- **O parser nunca inventa um fill.** Uma mensagem nossa que não se consegue ler conta em
  `nao_parseadas` em vez de virar um fill com campos adivinhados.

O envelope segue o que os bots deste repo já fazem (`xrp_bot_v9_4_1.py:2920`): dict ou lista de
dicts discriminados por `event_type`, com `asset_id`, auth L2 no connect, PING/PONG a 4 s, reconexão
com backoff. **Os campos da mensagem de trade não foram confirmados** contra a doc — os bots do repo
só tratam `market_resolved` e `taker_fee_rate_bps` no canal de user — por isso o parser exige
explicitamente o que precisa e conta o resto. O que falta para o `live` funcionar é **só o
transporte**: ligar o listener WS e encaminhar para `router.on_message()`. A lógica está testada
offline (`test_live.py`).

---

## 1. Objetivo

Construir uma ferramenta de LP farming para o programa de Liquidity Rewards do Polymarket, focada
em mercados extremos (um lado > 95¢, outro < 5¢, midpoint < 0.10). A referência é o `lp_book.py`
do vídeo: uma TUI que varre mercados, arma duas pernas à volta do midpoint, acumula rewards por
amostra, faz requote quando o mid se mexe, e mostra PnL de rewards mais o mark do inventário.

A versão a construir replica esse comportamento e esse aspeto, mas corrige o erro central da
referência e acrescenta o que falta. A referência coloca as ordens no midpoint ou muito perto
(0.0¢); isso é demonstravelmente sub-ótimo e foi exatamente o que levou o inventário do vídeo a
−17/−22. A versão nova coloca à distância matematicamente ótima do mid, calibrada a fills reais, e
seleciona mercados por rendimento líquido ajustado ao risco, não por tamanho bruto da pool.

Por que é superior, em três linhas:

- coloca em δ\* **decidido por mercado** — MID, INTERIOR ou BORDA conforme o `k` medido e a razão
  reward/custo (secção 0.5) — em vez de assumir o mid como a referência faz às cegas;
- calibra A e k a partir de fills reais e adapta-se à competição observada;
- seleciona e ordena mercados por líquido esperado, com guardrail de perda-por-fill sobre reward diário.

> **Corrigido:** a primeira linha dizia "coloca em δ\* (solução interior > 0), não em cima do mid".
> Falso — a implementação provou que o ótimo é frequentemente um canto. Ver secção 0.5. A diferença
> face ao vídeo não é *onde* se coloca, é que se **preça o custo de fill antes de decidir**: quando
> o resultado é o mid, é o mid com prova; o vídeo senta-se no mid sem a fazer, e nos mercados
> tóxicos é arrasado.

---

## 2. Referência a igualar (comportamento observado no vídeo)

Reproduzir esta mecânica, com dados reais em vez de sintéticos:

1. **Scan** — varre o universo de mercados (o vídeo mostra ~3255), rejeita tudo com pool abaixo de
   um limiar (`pool < $1.00/d`). O log de scan é uma lista de `skip ... under threshold`. Arma-se
   num subconjunto pequeno.
2. **Arm** — para um mercado com pool viva (ex.: $21.60/d), põe duas pernas de N shares a cavalo do
   mid. Mostra `q_min` (o mínimo ponderado das duas pernas), `share of pool %`, e projeção `$/mo`.
3. **Acumulação** — de X em X minutos regista um `credit` (amostra) e soma ao balanço. O vídeo
   mostra ~$0.68 por amostra de 20 min.
4. **Requote** — quando o mid se desloca, repõe as duas pernas (`legs reset to 4.4 / 5.2c`).
5. **Tracking** — cabeçalho com PNL, CAP (capital), MKT (mercados), Q, SHR (share), relógio.
   Rodapé com o tail do log. Painel com a reward band, o book (asks a vermelho acima do mid, bids
   a verde abaixo), e os multiplicadores por perna (`ask 0.2c x0.84`, `bid 0.6c x0.47`, etc.).
6. **Inventário** — quando uma perna enche, mostra `inventory N @ preço, mark preço, PnL`. No vídeo
   o bid a 0.0c do mid encheu, o mid caiu, e ficou `inventory 1000 @ 4.8c mark 3.1c −17.00`. Este é
   o modo de falha a eliminar.

Os multiplicadores do vídeo são consistentes com score quadrático e um `max_spread` de ~2¢. Não
assumir 2¢: ler o `max_spread` de cada mercado.

---

## 3. As correções quantitativas centrais

### 3.1 Função objetivo correta

Avellaneda-Stoikov e GLFT assumem que se ganha o spread ao executar: o fill é receita e o δ ótimo
equilibra taxa de execução contra margem por execução. No LP farming o sinal inverte-se: ganha-se
por estar pousado **sem** executar, o score é quadrático na proximidade ao mid, e o fill é custo
puro (seleção adversa). A utilidade por perna, em função da distância δ ao mid:

```
U(delta) = R * ((D - delta) / D)^2  -  C * A * exp(-k * delta)
```

- `D` — max_spread do mercado (lido da config, tipicamente 2–3¢);
- `R` — taxa de reward capturada no limite δ→0, escalada pela share esperada da pool (ver 4 e 5);
  é o termo que os multiplicadores do vídeo traçam;
- `A, k` — parâmetros da intensidade de fill `lambda(delta) = A * exp(-k*delta)`;
- `C` — perda esperada por fill adverso (mark contra a posição após encher).

> **Corrigido (implementação):** este objetivo está incompleto e degenera para a borda. Faltam-lhe a
> share explícita (`s/(s+q_others)`, que é o que dá retornos decrescentes ao size) e o fator de
> permanência na band `g(δ)` (perto da borda o drift do mid deita a perna fora e passa a marcar
> zero). Usar a forma completa da **secção 0.5** — é a que `lpbook/optimizer.py` implementa. Duas
> consequências que só aparecem com a forma completa: `C` escala com o size (a perda é por *fatia*
> cheia, e os fills são **parciais** — uma fatia da perna, não a perna toda; tratá-los como fills
> totais é o que fazia o custo explodir), e o size ótimo dimensiona-se ao **pool**, não à carteira.

### 3.2 δ\* e condição de solução interior

Derivar e resolver:

```
dU/ddelta = -2R (D - delta) / D^2  +  C k A exp(-k delta) = 0
=>  C k A exp(-k delta) = 2R (D - delta) / D^2
```

Existe δ\* > 0 (cotar no mid é sub-ótimo) sempre que, avaliado em δ=0:

```
C k A > 2R / D
```

Isto é, quando a poupança marginal de custo de fill ao recuar excede a perda marginal de reward no
mid. Em mercados com fluxo tóxico suficiente a desigualdade cumpre-se — é precisamente o regime
alvo. Se falhar (fills raros ou baratos, reward muito íngreme), o ótimo é o canto δ=0; a ferramenta
deve provar por mercado de que lado da desigualdade está, e nunca colocar no mid por omissão.

> **Corrigido, duas vezes.** (1) *Método:* "bissecção ou Newton em `[0, D]`" não funciona —
> `U'(D) > 0` sempre, logo não há bracketing quando a condição se cumpre, e no caso contrário a raiz
> encontrada é um **mínimo**. (2) *Tese:* a afirmação "existe δ\* > 0" **é falsa como está**. A
> condição `C k A > 2R/D` só prova que **o mid não é o ótimo** — não prova que o ótimo seja interior:
> pode ser a borda, e na maioria dos mercados finos é. Ver a tese dos três regimes na **secção 0.5**,
> confirmada empiricamente. Sob o objetivo completo, esta condição fechada deixa de valer: usar o
> teste numérico (`mid_suboptimal()` em `lpbook/optimizer.py:57`, que compara `U(ε)` com `U(0)`) e
> depois classificar o regime com `placement_regime()`.

**Nota de teoria de jogos:** a share de reward é normalizada contra concorrentes, por isso δ\* é a
melhor resposta dado o estado competitivo observado, não um ótimo de mundo estático. A adaptação
vem da calibração — A e k medidos aos fills reais já incorporam o que a concorrência está a fazer
agora.

### 3.3 Calibração de A e k (prioridade máxima)

Na referência não existe; é o que distingue colocar com fundamento de adivinhar. É o único
parâmetro que diz onde parar de apertar. Método:

1. medir tempos até fill para várias distâncias δ (a partir das ordens reais que enchem, ou de
   sondas controladas em size pequeno);
2. estimar `lambda(delta)` empírico por bucket de δ;
3. regressão linear de `ln(lambda)` vs `delta` → declive `-k`, interceção `ln(A)`;
4. re-estimar em janela deslizante; alimentar δ\* e a seleção de mercado.

> **Corrigido (implementação) — o passo 1 é onde isto falha na prática.** "Medir tempos até fill para
> várias distâncias δ" pressupõe que existem várias distâncias. Não existem: a política converge para
> um δ, todos os fills caem num bucket, e a regressão não devolve nada. **A variação de δ tem de ser
> imposta, não esperada** — e o passo 3, a regressão, tem de usar os buckets *sem* fills, que são os
> que fixam o `k`. Ver secção 0.7: foram precisas três correções (bucket por perna, MLE de Poisson
> em vez da regressão log-linear, e dithering de δ), e só as três juntas identificam o `k`. O
> dithering custa reward — 8.7% do líquido na medição — e é o preço de saber onde o ótimo está.
>
> **Nota (verificação repo):** para arrancar sem fills, o `ShadowFillEngine` (`xrp_bot_v9_4_1.py:820`)
> caminha a profundidade real do livro com latência e slippage. Buckets com menos de N observações
> não devem entrar na regressão.

### 3.4 Skew de inventário (reservation price A-S — este uso mantém-se)

O preço de reserva e o skew por inventário do A-S continuam válidos, e são a resposta que faltou no
vídeo. Ao encher e acumular o lado barato num preço a cair, deslocar ambas as cotações:

```
r = mid - q * gamma * sigma^2 * tau
```

`q` inventário (positivo = long do lado barato), `gamma` aversão, `sigma^2` variância do mid (vol
realizada + termo de salto), `tau` horizonte efetivo rolante. Inventário positivo → cotações para
baixo → bid mais longe do mid (compra menos), ask mais perto (vende mais) → escoa a posição. O
half-spread ótimo do A-S/GLFT **não** se usa: resolve o problema errado (fill como receita). Só o skew.

> **Corrigido (verificação repo):** o prompt original mandava "reaproveitar o jump-diffusion do
> `xrp_alpha.py`". **`xrp_alpha.py` não existe neste repo.** O código a reaproveitar é
> `compute_jump_diffusion_probability()` em `xrp_bot_v9_4_1.py:1637`, com os parâmetros
> `jump_lambda` / `jump_mu` / `jump_sigma` / `jump_terms` em `xrp_bot_v9_4_1.py:224-227`. Atenção
> ao que essa função devolve: é uma **probabilidade** de cruzamento sob mistura de Poisson, não uma
> variância. Para o termo `sigma^2` desta secção, extrair a variância total da mistura —
> `sigma^2 * tau + lambda * tau * (mu_j^2 + sigma_j^2)` — a partir dos mesmos parâmetros, em vez de
> chamar a função como está.
>
> **Corrigido (implementação):** a fórmula `r = mid − q·gamma·sigma^2·tau` aplicada com `q` em shares
> **rebenta a escala** — deu um shift de −895c num book cujo mid vive entre 1c e 10c. O produto cru
> não tem unidades comensuráveis com um preço em cêntimos. Normalizar ao cap de inventário e limitar:
> `r = mid − clamp(inv/inv_cap, −1, 1) · max_skew_c`, com `max_skew_c` explícito (0.8c na
> implementação). O sinal e a intuição do A-S mantêm-se — inventário no cap desloca exatamente
> `max_skew_c`, nunca mais.

### 3.5 Detetor de burst (Hawkes — muda de função)

Não serve para o spread ótimo. Serve para detetar clustering de fluxo (dinheiro informado) e
disparar alargamento ou retirada:

```
lambda(t) = mu + sum_{t_i < t} alpha * exp(-beta * (t - t_i))
```

Estimar a intensidade corrente pelos timestamps recentes de trades/fills. Quando `lambda(t)` dispara
acima de um múltiplo do baseline `mu`, alargar δ ou retirar as pernas. Estacionariedade
`alpha/beta < 1`. É um detetor de regime, não o gerador de cotações — é aqui que a memória
self-exciting paga.

---

## 4. Mecânica real de scoring do Polymarket (não inventar)

> **Estado de verificação:** as **fórmulas** desta secção continuam **não verificadas** —
> `docs.polymarket.com` está bloqueado pelo proxy de egress deste ambiente. São transportadas da
> fonte original (que as dá como confirmadas em agosto de 2026) e estão codificadas em
> `lpbook/scoring.py` com testes, mas os testes provam consistência interna, **não** que a fórmula
> seja a do Polymarket. Continua a ser a primeira coisa a reconfirmar contra a doc oficial — toda a
> economia da ferramenta assenta aqui.
>
> Os **nomes dos campos** do Gamma foram levantados por pesquisa web na sessão de construção e estão
> em uso em `lpbook/data_feed.py`: `rewardsMaxSpread`, `rewardsMinSize`, `rewardsDailyRate`,
> `clobTokenIds`, e o book em `/book?token_id=`. Não os re-verifiquei aqui (mesma rede bloqueada);
> a primeira chamada real ao Gamma confirma-os ou desmente-os em segundos.

- **Score por ordem**, dentro do `max_spread` do midpoint ajustado:

```
score_i = ((max_spread - spread_i) / max_spread)^2 * size_i
```

quadrático na distância, linear no size. Apertar bate pôr size, por larga margem.

- **Q por lado** = soma dos `score_i` das ordens desse lado.
- **Midpoint ajustado** = midpoint depois de filtrar ordens-poeira abaixo do `min_size` de incentivo
  (defesa contra fixar um mid falso). Usar o ajustado.
- **Combinação das duas pernas:**
  - midpoint em `[0.10, 0.90]`: `Q_min = max( min(Q_one, Q_two), max(Q_one/c, Q_two/c) )` —
    unilateral pontua a taxa reduzida (dividido por `c`, tipicamente 3);
  - midpoint em `[0, 0.10)` ou `(0.90, 1.0]`: `Q_min = min(Q_one, Q_two)` — bilateral obrigatório,
    unilateral pontua zero.
- O regime alvo (um lado < 5¢) cai **sempre** na banda extrema: duas pernas obrigatórias, sem
  crédito unilateral. É por isto que "uma perna não leva a lado nenhum". Em mercados binários, um
  bid em YES conta como ask em NO, por isso cotar os dois lados do book do outcome barato satisfaz
  o requisito.
- **Normalização:** `Q_min` da carteira a dividir pela soma dos `Q_min` de todos os makers na
  amostra → share; somar pelas amostras da época; normalizar de novo; multiplicar pela pool do mercado.
- **Amostragem** por minuto (documentação diz aleatória ~1/min; algumas fontes dizem por segundo —
  parametrizar). Uptime pontua diretamente: minutos em baixo são score perdido numa pool dividida
  por quem ficou de pé.
- **Pagamento** automático ~00:00 UTC do dia anterior, se o dia atingiu o mínimo de $1 — daí o filtro
  de pool. É um total diário, não um saldo acumulado.
- **Orçamentos de order/cancel** por signer, escalonados por volume maker de 30 dias. Um loop de
  requote apertado é cancel-heavy: dimensionar o loop ao tier, não descobrir o teto numa sessão volátil.

**Dados:** `gamma-api.polymarket.com` para lista de mercados e metadados de reward
(`rewardsDailyRate`, `rewardsMaxSpread`, `rewardsMinSize`, `clobTokenIds`); CLOB REST + WebSocket
para book e midpoint ao vivo. O endpoint US de incentives pode exigir chave; o Gamma internacional
expõe os mesmos campos de reward. O Gamma bloqueia User-Agents vazios — enviar um UA real.

> **Nota (verificação repo):** os três endpoints já são constantes no código
> (`xrp_bot_v9_4_1.py:206-209`) — reutilizar, não redigitar. E **não confundir** o `max_spread`
> desta secção com o `max_spread_cents` de `xrp_bot_v9_4_1.py:160`, que é um filtro de spread do
> livro para entradas direcionais, sem qualquer relação com rewards.

---

## 5. Seleção de mercado ajustada ao risco (a verdadeira melhoria)

A referência filtra só por pool > limiar. Insuficiente: o vídeo perdeu $17 num mercado que rendia
~$3/dia — um fill adverso custou perto de seis dias de reward daquele mercado. A métrica de desenho
não é o downside absoluto, é **perda-por-fill sobre taxa diária de reward**.

Ordenar e filtrar mercados por líquido esperado no δ\* de cada um:

```
E[líquido diário]     = R_diário(delta*)  -  C * Lambda_diário(delta*)
Lambda_diário(delta*) = A * exp(-k * delta*) * (tempo do dia)
rho                   = C * Lambda_diário(delta*) / R_diário(delta*)
```

- ranquear por `E[líquido diário]`, não pela pool bruta;
- rejeitar se `E[líquido diário] <= 0`;
- rejeitar se `rho` acima do limiar (~2–3 significa que os fills de um dia normal apagam vários dias
  de reward);
- também favorecer, como sinais de baixa competição: volume 24h baixo, spread de book largo,
  profundidade moderada (muitos LPs a competir = share menor).

> **Corrigido (implementação), dois pontos:**
>
> 1. **O size é uma variável de decisão, não um input.** A receita está limitada pelo pool e satura
>    na share (`s/(s+q_others)`), mas o risco de inventário cresce linearmente com o size. Meter $20
>    — ou $5000 — num pool de $9/dia é risco puro sem receita adicional. Otimizar **conjuntamente
>    `(size, δ)`**: `lpbook/selector.py:58-70` varre 16 sizes log-espaçados entre o `min_size` de
>    incentivo e o teto da carteira, resolve o δ\* de cada um, e fica com o par de melhor net.
> 2. **`rho_max` ~2–3 é frouxo demais para conta pequena.** A implementação usa **0.6** por omissão
>    (`--rho-max`): exige que o reward esperado bata a perda adversa esperada com ~67% de folga.
>    O raciocínio: com $20 não há margem para absorver um golpe de inventário. Os 2–3 do texto
>    original só fazem sentido numa conta grande e tolerante a inventário — a escolha é do operador,
>    mas o default deve ser o esquisito.
>
> `E[líquido diário] <= 0` é o mesmo veredito que o regime BORDA (secção 0.5, onde `U(D) < 0` por
> construção). Implementar uma vez, no `optimizer.py`, e o `selector.py` consome — não duplicar a
> conta com sinais que podem divergir.

---

## 6. Arquitetura e stack

> **Corrigido (verificação repo):** o original dizia "reaproveitar os padrões já existentes (bots
> XRP): IPC por JSON atómico, `control.json` como kill-switch, deploy systemd, painel de telemetria
> Streamlit". Verificado ficheiro a ficheiro: **`control.json`, Streamlit e systemd não existem
> neste repo** (zero ocorrências). O que existe e serve está na secção 0.1. Tratar o `control.json`,
> o painel e o deploy como **construção nova**, não como integração.

Layout modular enxuto (pode colapsar em menos ficheiros se ficar mais limpo):

- `data_feed.py` — Gamma (metadados de reward) + CLOB WS (book/mid ao vivo); cache; reconexão
  automática. *Endpoints já em `xrp_bot_v9_4_1.py:206-209`.*
- `scoring.py` — `max_spread`, midpoint ajustado, Q por lado, `Q_min` com as duas bandas, share
  normalizada. Funções puras, testáveis.
- `optimizer.py` — δ\* pelo algoritmo da secção 0.4, condição de solução interior, calibração A/k em
  janela deslizante, skew de inventário A-S.
- `flow.py` — intensidade Hawkes e sinal de burst → alargar/retirar.
- `selector.py` — E[líquido diário], `rho`, ranking e filtro de mercados.
- `book_engine.py` — por mercado: mantém as duas pernas em δ\*, requote no drift, aplica skew e
  sinais de fluxo, respeita caps e budgets de cancel.
- `execution.py` — camada separável e explicitamente gated: paper (fills simulados contra book real
  + modelo de fill) vs live (ordens CLOB reais atrás de flag e `control.json`). *Paper: adaptar o
  `ShadowFillEngine` (`xrp_bot_v9_4_1.py:820`). Live: `py_clob_client` como em
  `xrp_true_market_maker_v5_3_1.py:42-44`, credenciais via `_load_secrets_file()`
  (`xrp_bot_v9_4_1.py:94`).*
- `tui.py` — render ao vivo (Rich `Live`+`Layout`, ou Textual se se justificar), reproduzindo o
  painel do vídeo. *Sem base no repo — `rich` é dependência nova.*
- `state.py` — estado em JSON atómico + kill-switch. *Padrão em `TradeStateManager`
  (`xrp_bot_v9_4_1.py:1039`), com a correção da secção 0.1: `os.replace()` direto, sem a janela do
  `.bak`. O `control.json` segue a forma do `config_hot_reload_loop()`
  (`xrp_true_market_maker_v5_3_1.py:195`): poll de `mtime`, allowlist de campos.*

**Modos:** `scan` (ranking), `paper` (arma top-N, simula, PnL líquido), `live` (coloca ordens reais,
gated), `replay` (snapshots históricos para validar δ\* e calibração).

---

## 7. Gestão de risco e segurança

- Paper por omissão. Live só com flag explícita e `control.json` ativo.
- Hard cap de inventário por mercado e agregado; auto-flatten ao encher acima do cap.
- Skew de inventário sempre ativo (secção 3.4) — nunca acumular passivamente um ativo a cair, que
  foi a falha do vídeo.
- Kill-switch global por `control.json`; retirada de pernas em burst de fluxo (secção 3.5).
- Size por ordem limitado; loop de requote dimensionado ao tier de order/cancel budget. *(Por fazer
  na implementação — secção 0.6, achado 3.)*
- Nunca cotar em δ=0 sem ter resolvido o regime desse mercado (secção 0.5). Se o resultado for MID,
  cotar no mid — com a prova feita, ao contrário do vídeo. O que se proíbe é o mid **por omissão**.
- Nunca correr com `k` assumido em `live`: sem calibração identificada (secção 0.6, achado 1) o
  regime é uma adivinha, e é o regime que decide tudo.

---

## 8. TUI — o que renderizar

Igualar o painel do vídeo e acrescentar o que a referência esconde:

- cabeçalho: PNL (líquido), CAP, MKT, Q, SHR, relógio;
- reward band com o book (asks vermelho acima do mid, bids verde abaixo) e os multiplicadores por
  perna `((D-δ)/D)^2`;
- as duas pernas atuais em δ\* marcadas no book (`YOU N`);
- δ\* e a folga face ao mid como valores de primeira classe (o que a referência nunca mostra —
  mostra só a colocação perto do mid);
- linha de seleção adversa sempre visível: perda-por-fill, `rho`, e líquido vs bruto lado a lado (a
  referência só mostra o mark do inventário depois do estrago);
- rodapé: tail de log (`scan/skip`, `arm`, `credit`, `requote`, `fill`, `flatten`, `withdraw`);
- por mercado armado: pool, share, `E[líquido diário]`, `$/mo` líquido.

---

## 9. Pedido de construção (entregáveis concretos)

Ao executar este prompt, produzir:

1. Os módulos da secção 6, código PT-PT conciso sem placeholders.
2. `scoring.py` com as duas bandas de midpoint e testes que reproduzem os multiplicadores do tipo do
   vídeo para um `max_spread` dado.
3. `optimizer.py` com o solver de δ\* (algoritmo da secção 0.4), a verificação da condição de solução
   interior por mercado, e a calibração A/k a partir de fills.
4. `selector.py` a ranquear por `E[líquido diário]` com o guardrail `rho`.
5. TUI (secção 8) com modo `paper` a correr sobre dados reais + modelo de fill, e a linha de seleção
   adversa sempre presente.
6. Modo `live` isolado atrás de flag e `control.json`, reutilizando a infra CLOB existente e o
   `control.json` novo modelado no hot-reload da secção 0.2.

**I/O:** entrada `bankroll`, `N` mercados, limiar de pool, `gamma`, cap de inventário, `rho_max`,
modo. Estado em JSON atómico. Kill-switch `control.json`.

**Critérios de aceitação:**

- lê `max_spread`/`min_size`/pool por mercado do Gamma, nada hardcoded;
- nunca coloca em δ=0 sem ter resolvido o regime do mercado;
- o solver de δ\* reproduz os **três regimes** (secção 0.5) em testes parametrizados, incluindo os
  dois cantos;
- a calibração de A/k é **identificável**: o bucket é indexado pela distância real de cada perna e
  há dithering de δ, de modo que uma corrida real produz ≥2 buckets povoados (secção 0.6);
- otimiza `(size, δ)` em conjunto — o size dimensiona-se ao pool, não à carteira;
- reporta líquido, com a seleção adversa sempre à vista;
- trata a regra de bilateral obrigatório na banda extrema;
- `rho` rejeita mercados tóxicos.

---

## 10. O que NÃO fazer

- Não colocar as pernas no mid nem a 0.0¢ por omissão (o erro do vídeo).
- Não assumir `max_spread` = 2¢ nem qualquer valor fixo — ler por mercado.
- Não usar o half-spread do A-S/GLFT como spread de cotação (resolve fill como receita; aqui fill é
  custo). Só o skew de inventário.
- Não tratar fills como receita em lado nenhum da contabilidade.
- Não hardcodar bankroll — parâmetro, como o `CAP` do vídeo.
- Não ranquear mercados por pool bruta — ranquear por líquido esperado.
- Não deixar `execution` live sem gate por flag e `control.json`.
- **Não bissectar `dU/dδ` cegamente em `[0, D]`** (ver secção 0.4) — a raiz encontrada assim pode ser
  o mínimo. Com o objetivo completo, otimizar numericamente e comparar com os cantos.
- **Não afirmar que existe sempre um δ\* interior** (secção 0.5) — e não confundir "o mid não é
  ótimo" com "o ótimo é interior". Na maioria dos mercados finos o ótimo é a borda, que significa
  não farmar.
- **Não tratar os fills como totais** — são parciais, uma fatia da perna. Tratá-los como a perna
  inteira faz o custo estimado explodir e rejeita tudo.
- **Não dimensionar o size pela bankroll** — dimensionar pelo pool.
- **Não aplicar o skew A-S cru com `q` em shares** — normalizar ao cap e limitar (secção 3.4).
- **Não procurar `xrp_alpha.py`, `control.json`, painel Streamlit ou units systemd neste repo** — não
  existem (secção 0.2).

---

## 11. Honestidade de expectativas (embeber na ferramenta)

O "$1500/mês" e o "$3.5 por cada $100/dia" da fonte original são **brutos e pré-seleção-adversa**.
Fontes independentes de 2026 põem contas de $10K–$50K a render $200–$800/dia em rewards brutos, mas
líquidos anualizados de 15–35% depois de gerir inventário — o delta é exatamente a seleção adversa.
3.5%/dia seriam ~1150%/ano, o que não sobrevive ao contacto com fills normais sem seleção de mercado
e disciplina de inventário. A ferramenta deve mostrar sempre o líquido e nunca a colocação no mid,
porque é aí que a diferença entre a promessa e o resultado se decide.

> **Estado de verificação:** estes números vêm da fonte original e **não foram verificados** nesta
> sessão. Tratar como ordem de grandeza, não como referência.

---

## 12. Nota de segurança sobre este repo

`secrets.txt` está **versionado no git** (confirmado por `git ls-files`) e o repo não tem `.gitignore`.
Quaisquer credenciais lá dentro estão no histórico e continuam recuperáveis mesmo que o ficheiro seja
apagado num commit futuro. Antes de correr o modo `live` desta ferramenta — que reutiliza
`_load_secrets_file()` e portanto o mesmo ficheiro:

1. rodar todas as chaves e API creds que alguma vez estiveram nesse ficheiro;
2. adicionar `.gitignore` (`secrets.txt`, `.DS_Store`, `*.log`, `*.jsonl`, `trade_state.json*`,
   `control.json`, `state.json`) e retirar `secrets.txt` e `.DS_Store` do índice com
   `git rm --cached`;
3. `chmod 600 secrets.txt` — `_load_secrets_file()` (`xrp_bot_v9_4_1.py:94`) já avisa se as
   permissões estiverem laxas, mas só avisa.

Deliberadamente **não** alterado nesta mudança: é uma decisão de operador, e reescrever o histórico
tem custos que não cabem aqui.

---

## 13. Resumo do estado de verificação

| Bloco | Estado |
|---|---|
| §0 mapa do repo (existe / não existe / armadilha de nomes) | **Verificado** contra o código, com caminhos e linhas, em 29/08/2026 |
| §0.4 correção do solver de δ\* | **Verificado analiticamente** — `U'(D) > 0` para quaisquer parâmetros, logo a receita original não bracketa. Superado pelo objetivo completo da §0.5 |
| §0.5 três regimes (MID / INTERIOR / BORDA) | **Verificado** — 8/8 testes de `lpbook/` passam e corri o varrimento da tabela eu próprio. `U(BORDA) < 0` é teorema |
| §0.6 achados na implementação | **Verificado** — calibração não identificada reproduzida numa corrida `paper` real (priores intactos ao fim de 7 fills); ausência de WS/asyncio e de budget de order/cancel confirmada por leitura do código |
| §0.7 correção da calibração | **Verificado** — 12/12 testes passam; `k` recuperado em 5 seeds × 72 h, erro absoluto médio 0.433 → 0.207. Amostra pequena (n=5): a direção é consistente, a amplitude ótima do dithering **não** está determinada |
| §0.7 perna fora da band com skew | **Verificado** — reproduzido (2.4c numa band de 2.0c) e corrigido com clamp, coberto por teste |
| §0.8 δ assimétrico destrói o `Q_min` | **Verificado** — calculado com o `scoring.py` da ferramenta: 68% / 23% / 1.6% do reward retido. É aritmética da regra `min()`, não estimativa |
| §0.8 sinal de toxicidade | **NÃO validado** — impossível de validar no harness atual (mid é martingale). A/B em 3 seeds dá pior com o sinal ligado, como a teoria prevê. Desligado por omissão |
| §0.8 rebate de maker (20 bps) | **NÃO verificado** — proveniência é o default de um bot deste repo, não a doc do Polymarket. A 0 bps por omissão; valeria 0.5–1.3% do adverso |
| Números do `scan` a $20 (4 rejeitados, 1 armado, +$2.89/d, rho 0.52) | **Verificado** — reproduzidos localmente |
| §3.4 âncora do jump-diffusion | **Verificado** — `xrp_alpha.py` não existe; o código está em `xrp_bot_v9_4_1.py:1637` |
| §6 reaproveitamentos (estado, fills, endpoints, CLOB, secrets) | **Verificado** — todos existem, âncoras em §0.1 |
| §6 `control.json`, Streamlit, systemd, TUI | **Verificado que NÃO existem** — construção nova |
| §4 fórmulas de scoring do Polymarket | **NÃO verificado** — `docs.polymarket.com` bloqueado pelo proxy de egress. Codificado em `lpbook/scoring.py`, mas os testes provam consistência interna, não conformidade com a doc |
| §4 nomes dos campos do Gamma (`rewardsMaxSpread`…) | **Não re-verificado aqui** — levantados por pesquisa web na sessão de construção; a primeira chamada real ao Gamma confirma |
| `scan` / `live` contra a rede real | **Nunca exercitados** — rede do Polymarket bloqueada. `paper` e `replay` correm offline |
| §11 números de rendimento | **NÃO verificado** — transportado da fonte original |
| §12 `secrets.txt` versionado | **Verificado** — aparece em `git ls-files`, sem `.gitignore` no repo |
