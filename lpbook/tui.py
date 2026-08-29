"""TUI (Rich). Reproduz o painel do lp_book.py e acrescenta o que a referencia
esconde: delta* e a folga face ao mid, e a linha de selecao adversa sempre
visivel (perda/fill, rho, liquido vs bruto).
"""
from __future__ import annotations
from collections import deque

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from scoring import order_score


def _bar(size, smax, width=16, ch="\u2588"):
    n = int(round(width * min(size / smax, 1.0))) if smax > 0 else 0
    return ch * n + "\u00b7" * (width - n)


def reward_band(st, mid_c, bids, asks):
    d = st.max_spread_c
    smax = max([sz for _, sz in bids + asks] + [st.size, 1.0])
    lines = []
    for lvl, sz in sorted(asks, key=lambda x: -x[0]):
        spread = abs(lvl - mid_c)
        inb = spread <= d
        mult = ((d - spread) / d) ** 2 if inb else 0.0
        you = st.ask and abs(st.ask.level_c - lvl) < 0.2
        tag = "  [black on red] YOU [/]" if you else ""
        col = "red" if inb else "grey42"
        lines.append(Text.from_markup(
            f"[{col}]{lvl:5.1f}c [/][{col}]{_bar(sz, smax)}[/] "
            f"[grey54]{sz:5.0f}[/]  [red]x{mult:.2f}[/]{tag}" if inb else
            f"[grey42]{lvl:5.1f}c {_bar(sz, smax)} {sz:5.0f}[/]"))
    lines.append(Text.from_markup(f"[bold white]        mid {mid_c:4.1f}c[/]  "
                                  f"[yellow]delta* {st.delta_c:.2f}c[/]"))
    for lvl, sz in sorted(bids, key=lambda x: -x[0]):
        spread = abs(mid_c - lvl)
        inb = spread <= d
        mult = ((d - spread) / d) ** 2 if inb else 0.0
        you = st.bid and abs(st.bid.level_c - lvl) < 0.2
        tag = "  [black on green] YOU [/]" if you else ""
        col = "green" if inb else "grey42"
        lines.append(Text.from_markup(
            f"[{col}]{lvl:5.1f}c [/][{col}]{_bar(sz, smax)}[/] "
            f"[grey54]{sz:5.0f}[/]  [green]x{mult:.2f}[/]{tag}" if inb else
            f"[grey42]{lvl:5.1f}c {_bar(sz, smax)} {sz:5.0f}[/]"))
    return Panel(Group(*lines), title="[yellow]reward band[/]", border_style="grey35")


def header(st, mid_c, pnl_net, cap, share, clock):
    rc = {"MID": "grey54", "INTERIOR": "green", "BORDA": "red"}.get(st.regime, "grey54")
    reg = f"[{rc}]{st.regime}[/]"
    wd = "  [red]WITHDRAWN[/]" if getattr(st, "withdrawn_flag", False) else ""
    t = Text.from_markup(
        f"[bold]LP BOOK PRO[/]  [bold white]{st.market_id}[/]      "
        f"[grey54]pool ${st.daily_pool:.2f}/d[/]\n"
        f"PNL [bold green]{pnl_net:+.2f}[/]  CAP {cap:.0f}  "
        f"Q [white]{share*100:5.1f}%[/]  delta* [yellow]{st.delta_c:.2f}c[/] {reg}  "
        f"inv {st.inv:.0f}@{st.avg_c:.1f}c  {clock}{wd}")
    return t


def adverse_line(reward_daily, cost_daily, per_fill, rho, rho_max, mark):
    net = reward_daily - cost_daily
    ok = rho <= rho_max
    col = "green" if ok else "red"
    return Text.from_markup(
        f"[bold]selecao adversa[/]  perda/fill [red]${per_fill:.2f}[/]  "
        f"rho [{col}]{rho:.2f}[/]/{rho_max:.2f}  "
        f"reward/d [green]${reward_daily:.2f}[/]  liquido/d "
        f"[{'green' if net>0 else 'red'}]${net:+.2f}[/]  "
        f"mark [{'green' if mark>=0 else 'red'}]{mark:+.2f}[/]")


def log_panel(log: deque):
    tbl = Table.grid(padding=(0, 1))
    for line in list(log)[-6:]:
        tbl.add_row(Text.from_markup(line))
    return Panel(tbl, border_style="grey27", title="[grey54]log[/]")


def render(st, mid_c, bids, asks, pnl_net, cap, share, clock,
           reward_daily, cost_daily, per_fill, rho, rho_max, mark, log):
    return Group(
        header(st, mid_c, pnl_net, cap, share, clock),
        reward_band(st, mid_c, bids, asks),
        adverse_line(reward_daily, cost_daily, per_fill, rho, rho_max, mark),
        log_panel(log),
    )
