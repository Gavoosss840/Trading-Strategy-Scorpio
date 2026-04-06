"""
Reports — Terminal (ASCII) and HTML report generation
All HTML files are saved in the output/ folder.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from .config import ZSCORE_ENTRY, ZSCORE_EXIT
from .portfolio import PortfolioTracker

logger = logging.getLogger(__name__)

LINE = '─' * 80
OUTPUT_DIR = 'output'


def _ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


class ReportGenerator:
    """
    Generates performance reports in two formats:
    • Terminal : formatted ASCII tables
    • HTML     : full-page dashboard with Chart.js equity curve
    """

    # ── Terminal reports ──────────────────────────────────────────────

    def print_spreads(self, spread_results: List[Dict]):
        print(f'\n{"ANALYSE DES SPREADS":^80}')
        print(LINE)
        print(f'{"PAIRE":<20} {"z-score":>8} {"SPREAD":>8} {"MOYENNE":>8} '
              f'{"DÉVIATION":>10} {"SIGNAL":<22} {"CONF":>6}')
        print(LINE)
        for r in spread_results:
            if r['spread_current'] is None:
                continue
            flag  = '▲' if r['z_score'] >  ZSCORE_ENTRY else \
                    ('▼' if r['z_score'] < -ZSCORE_ENTRY else ' ')
            print(f'{r["pair"]:<20} {r["z_score"]:>+8.2f} '
                  f'{r["spread_current"]:>+8.3f}% {r["spread_mean"]:>+8.3f}% '
                  f'{r["deviation_bps"]:>+9.1f}bps  '
                  f'{flag} {r["signal"]:<20} {r["confidence"]:>5.0f}%')
        print(LINE)

    def print_positions(self, portfolio: PortfolioTracker):
        print(f'\n{"POSITIONS OUVERTES":^80}')
        print(LINE)
        if not portfolio.positions:
            print('  Aucune position ouverte.')
        for pid, pos in portfolio.positions.items():
            ll, ls = pos['leg_long'], pos['leg_short']
            print(f'  {pos["pair"]:20s} | Entrée: {pos["entry_time"][:16]} '
                  f'| z_entrée: {pos["z_entry"]:+.2f}')
            print(f'    LONG  {ll["futures"]:6s} @ {ll["entry_price"]}  '
                  f'TP={ll["tp"]}  SL={ll["sl"]}  qty={ll["qty"]}')
            print(f'    SHORT {ls["futures"]:6s} @ {ls["entry_price"]}  '
                  f'TP={ls["tp"]}  SL={ls["sl"]}  qty={ls["qty"]}')
        print(LINE)

    def print_summary(self, portfolio: PortfolioTracker):
        print(f'\n{"RÉSUMÉ PERFORMANCE":^80}')
        print(LINE)
        print(f'  Trades clôturés : {len(portfolio.closed_trades)}')
        print(f'  P&L total       : ${portfolio.total_pnl():>+12,.0f}')
        print(f'  Win rate        : {portfolio.win_rate():>6.1f}%')
        print(f'  Max drawdown    : ${portfolio.max_drawdown():>+12,.0f}')
        print(f'  Sharpe (annuel) : {portfolio.sharpe():>6.2f}')
        print(LINE)

    def print_backtest(self, metrics: Dict):
        if not metrics:
            return
        print(f'\n{"RÉSULTATS BACKTEST":^80}')
        print(LINE)
        print(f'  Période         : {metrics["date_from"]} -> {metrics["date_to"]}')
        print(f'  Capital initial : ${metrics["initial_cap"]:>12,.0f}')
        print(f'  Capital final   : ${metrics["final_equity"]:>12,.0f}')
        print(f'  Rendement total : {metrics["total_return"]:>+7.2f}%')
        print(f'  Sharpe ratio    : {metrics["sharpe"]:>7.2f}')
        print(f'  Max Drawdown    : ${metrics["max_drawdown"]:>+12,.0f} '
              f'({metrics["max_dd_pct"]:+.2f}%)')
        print(f'  Trades          : {metrics["n_trades"]}  '
              f'(win rate {metrics["win_rate"]:.1f}%)')
        print(LINE)
        if metrics.get('trades'):
            print(f'\n  {"TRADE":<22} {"ENTRÉE":<12} {"SORTIE":<12} '
                  f'{"RAISON":<15} {"P&L":>10}')
            print('  ' + '─' * 72)
            for t in metrics['trades'][-20:]:
                print(f'  {t["pair"]:<22} {t["entry_date"]:<12} '
                      f'{t.get("exit_date", "ouvert"):<12} '
                      f'{t.get("exit_reason", ""):<15} '
                      f'${t.get("pnl", 0):>+9,.0f}')

    # ── HTML report ───────────────────────────────────────────────────

    def save_html(self, portfolio: PortfolioTracker,
                  spread_results: List[Dict],
                  backtest: Optional[Dict] = None,
                  filepath: str = 'report.html') -> str:
        _ensure_output_dir()
        filepath = os.path.join(OUTPUT_DIR, os.path.basename(filepath))
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # ── Equity curve data ─────────────────────────────────────────
        chart_html = ''
        if backtest and backtest.get('equity_curve'):
            eq     = backtest['equity_curve']
            dates  = backtest.get('equity_dates', [''] * len(eq))
            labels = json.dumps([d for d in dates if d])
            values = json.dumps(eq[1:] if len(eq) > len([d for d in dates if d]) else eq)
            cap    = backtest['initial_cap']
            # Drawdown series
            import operator
            peak_list, dd_list = [], []
            pk = eq[0]
            for v in eq:
                if v > pk:
                    pk = v
                peak_list.append(pk)
                dd_list.append((v - pk) / cap * 100)
            dd_values  = json.dumps(dd_list[1:] if len(dd_list) > 1 else dd_list)

            chart_html = f'''
<div class="section">
  <h2>&#x1F4C8; Courbe Equity</h2>
  <div class="chart-box"><canvas id="eqChart"></canvas></div>
</div>
<div class="section">
  <h2>&#x1F53B; Drawdown (%)</h2>
  <div class="chart-box"><canvas id="ddChart"></canvas></div>
</div>
<script>
(function(){{
  var labels = {labels};
  var eqData = {values};
  var ddData = {dd_values};
  var capLine = Array(labels.length).fill({cap});

  function makeChart(id, datasets, yLabel) {{
    var ctx = document.getElementById(id).getContext('2d');
    new Chart(ctx, {{
      type: 'line',
      data: {{ labels: labels, datasets: datasets }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        animation: false,
        plugins: {{ legend: {{ labels: {{ color: '#c9d1d9' }} }} }},
        scales: {{
          x: {{ ticks: {{ color: '#8b949e', maxTicksLimit: 12 }},
               grid: {{ color: '#21262d' }} }},
          y: {{ ticks: {{ color: '#8b949e' }},
               grid: {{ color: '#21262d' }},
               title: {{ display: true, text: yLabel, color: '#8b949e' }} }}
        }}
      }}
    }});
  }}

  makeChart('eqChart', [
    {{ label: 'Equity ($)', data: eqData,
       borderColor: '#58a6ff', borderWidth: 2,
       fill: true, backgroundColor: 'rgba(88,166,255,0.07)',
       pointRadius: 0, tension: 0.1 }},
    {{ label: 'Capital initial', data: capLine,
       borderColor: '#30363d', borderWidth: 1,
       borderDash: [6,4], pointRadius: 0 }}
  ], 'USD');

  makeChart('ddChart', [
    {{ label: 'Drawdown (%)', data: ddData,
       borderColor: '#f85149', borderWidth: 1.5,
       fill: true, backgroundColor: 'rgba(248,81,73,0.10)',
       pointRadius: 0, tension: 0.1 }}
  ], '%');
}})();
</script>'''

        # ── Stats cards (backtest) ────────────────────────────────────
        stats_html = ''
        if backtest:
            ret_color = '#3fb950' if backtest['total_return'] >= 0 else '#f85149'
            dd_color  = '#f85149'
            stats_html = f'''
<div class="section">
  <h2>&#x1F4CA; Résultats Backtest  <span class="sub">{backtest["date_from"]} → {backtest["date_to"]}</span></h2>
  <div class="cards">
    <div class="card">
      <div class="card-label">Rendement total</div>
      <div class="card-value" style="color:{ret_color}">{backtest["total_return"]:+.2f}%</div>
    </div>
    <div class="card">
      <div class="card-label">Capital final</div>
      <div class="card-value">${backtest["final_equity"]:,.0f}</div>
    </div>
    <div class="card">
      <div class="card-label">Sharpe ratio</div>
      <div class="card-value">{backtest["sharpe"]:.2f}</div>
    </div>
    <div class="card">
      <div class="card-label">Max Drawdown</div>
      <div class="card-value" style="color:{dd_color}">{backtest["max_dd_pct"]:+.2f}%</div>
    </div>
    <div class="card">
      <div class="card-label">Win Rate</div>
      <div class="card-value">{backtest["win_rate"]:.1f}%</div>
    </div>
    <div class="card">
      <div class="card-label">Trades</div>
      <div class="card-value">{backtest["n_trades"]}</div>
    </div>
  </div>
</div>'''

        # ── Trades table (backtest) ───────────────────────────────────
        bt_trades_html = ''
        if backtest and backtest.get('trades'):
            rows = ''
            for t in backtest['trades']:
                pnl   = t.get('pnl', 0)
                color = '#3fb950' if pnl >= 0 else '#f85149'
                reason_badge = (
                    '<span class="badge-tp">TP</span>'
                    if 'TP' in t.get('exit_reason', '')
                    else '<span class="badge-sl">SL</span>'
                )
                rows += (
                    f'<tr>'
                    f'<td>{t["pair"]}</td>'
                    f'<td>{t["entry_date"]}</td>'
                    f'<td>{t.get("exit_date","—")}</td>'
                    f'<td>{t.get("signal","")}</td>'
                    f'<td>{reason_badge}</td>'
                    f'<td>{t.get("entry_z", 0):+.2f}</td>'
                    f'<td style="color:{color};font-weight:bold">'
                    f'${pnl:+,.0f}</td>'
                    f'</tr>\n'
                )
            bt_trades_html = f'''
<div class="section">
  <h2>&#x1F4CB; Historique des Trades</h2>
  <table>
    <tr><th>Paire</th><th>Entrée</th><th>Sortie</th><th>Signal</th>
        <th>Raison</th><th>z entrée</th><th>P&amp;L</th></tr>
    {rows}
  </table>
</div>'''

        # ── Spread table ──────────────────────────────────────────────
        spread_rows = ''
        for r in spread_results:
            if r['spread_current'] is None:
                continue
            sig_color = '#f85149' if abs(r['z_score']) > ZSCORE_ENTRY else \
                        ('#e3b341' if abs(r['z_score']) > ZSCORE_ENTRY * 0.7 else '#8b949e')
            spread_rows += (
                f'<tr>'
                f'<td>{r["pair"]}</td>'
                f'<td style="color:{sig_color};font-weight:bold">{r["z_score"]:+.2f}</td>'
                f'<td>{r["spread_current"]:+.3f}%</td>'
                f'<td>{r["spread_mean"]:+.3f}%</td>'
                f'<td>{r["deviation_bps"]:+.1f} bps</td>'
                f'<td style="color:{sig_color}">{r["signal"]}</td>'
                f'<td>{r["confidence"]:.0f}%</td>'
                f'</tr>\n'
            )

        spreads_section = ''
        if spread_rows:
            spreads_section = f'''
<div class="section">
  <h2>&#x1F30D; Spreads Inter-pays</h2>
  <table>
    <tr><th>Paire</th><th>z-score</th><th>Spread</th><th>Moyenne</th>
        <th>Déviation</th><th>Signal</th><th>Conf</th></tr>
    {spread_rows}
  </table>
</div>'''

        # ── Open positions ────────────────────────────────────────────
        pos_rows = ''
        for pid, pos in portfolio.positions.items():
            ll, ls = pos['leg_long'], pos['leg_short']
            pos_rows += (
                f'<tr><td>{pos["pair"]}</td>'
                f'<td>{pos["entry_time"][:16]}</td>'
                f'<td>{pos["z_entry"]:+.2f}</td>'
                f'<td>LONG {ll["futures"]} / SHORT {ls["futures"]}</td>'
                f'<td>{ll["tp"]} / {ls["tp"]}</td>'
                f'<td>{ll["sl"]} / {ls["sl"]}</td></tr>\n'
            )

        positions_section = f'''
<div class="section">
  <h2>&#x1F4BC; Positions Ouvertes ({len(portfolio.positions)})</h2>
  <table>
    <tr><th>Paire</th><th>Entrée</th><th>z entrée</th>
        <th>Legs</th><th>TP</th><th>SL</th></tr>
    {pos_rows if pos_rows else "<tr><td colspan='6' style='text-align:center;color:#8b949e'>Aucune position ouverte</td></tr>"}
  </table>
</div>'''

        # ── Live perf ─────────────────────────────────────────────────
        live_section = ''
        if portfolio.closed_trades:
            tpnl = portfolio.total_pnl()
            c = '#3fb950' if tpnl >= 0 else '#f85149'
            live_section = f'''
<div class="section">
  <h2>&#x1F4B0; Performance Live</h2>
  <div class="cards">
    <div class="card"><div class="card-label">P&amp;L Total</div>
      <div class="card-value" style="color:{c}">${tpnl:+,.0f}</div></div>
    <div class="card"><div class="card-label">Trades fermés</div>
      <div class="card-value">{len(portfolio.closed_trades)}</div></div>
    <div class="card"><div class="card-label">Win Rate</div>
      <div class="card-value">{portfolio.win_rate():.1f}%</div></div>
    <div class="card"><div class="card-label">Max Drawdown</div>
      <div class="card-value" style="color:#f85149">${portfolio.max_drawdown():+,.0f}</div></div>
    <div class="card"><div class="card-label">Sharpe</div>
      <div class="card-value">{portfolio.sharpe():.2f}</div></div>
  </div>
</div>'''

        html = f'''<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scorpio Bond Arb — {now}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
    background: #0d1117;
    color: #c9d1d9;
    padding: 2rem;
    font-size: 14px;
  }}
  .header {{
    border-bottom: 1px solid #30363d;
    padding-bottom: 1rem;
    margin-bottom: 2rem;
  }}
  .header h1 {{
    font-size: 1.6rem;
    color: #58a6ff;
    font-weight: 600;
  }}
  .header p {{ color: #8b949e; margin-top: .3rem; font-size: .85rem; }}
  .section {{
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 1.25rem 1.5rem;
    margin-bottom: 1.5rem;
  }}
  .section h2 {{
    font-size: 1rem;
    color: #e6edf3;
    font-weight: 600;
    margin-bottom: 1rem;
  }}
  .section h2 .sub {{
    font-size: .8rem;
    color: #8b949e;
    font-weight: 400;
    margin-left: .5rem;
  }}
  .cards {{
    display: flex;
    flex-wrap: wrap;
    gap: .75rem;
  }}
  .card {{
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: .75rem 1.25rem;
    min-width: 130px;
    flex: 1;
  }}
  .card-label {{ color: #8b949e; font-size: .75rem; margin-bottom: .25rem; }}
  .card-value {{ color: #c9d1d9; font-size: 1.3rem; font-weight: 700; }}
  table {{
    border-collapse: collapse;
    width: 100%;
    font-size: .85rem;
  }}
  th {{
    background: #0d1117;
    padding: 7px 12px;
    text-align: left;
    color: #8b949e;
    border-bottom: 1px solid #30363d;
    font-weight: 600;
    white-space: nowrap;
  }}
  td {{
    padding: 6px 12px;
    border-bottom: 1px solid #21262d;
    white-space: nowrap;
  }}
  tr:hover td {{ background: #1c2128; }}
  .badge-tp {{
    background: #1a4731; color: #3fb950;
    font-size: .7rem; padding: 2px 7px;
    border-radius: 10px; font-weight: 600;
  }}
  .badge-sl {{
    background: #3d1a1c; color: #f85149;
    font-size: .7rem; padding: 2px 7px;
    border-radius: 10px; font-weight: 600;
  }}
  .chart-box {{
    position: relative;
    height: 300px;
  }}
</style>
</head>
<body>
<div class="header">
  <h1>&#x1F9FF; Scorpio — Government Bond Arbitrage</h1>
  <p>Rapport généré le <strong>{now}</strong> &nbsp;·&nbsp;
     Spread Inter-pays × NPV/DCF Confirmation</p>
</div>

{stats_html}
{chart_html}
{bt_trades_html}
{spreads_section}
{positions_section}
{live_section}

</body>
</html>'''

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info(f'[REPORT] Saved -> {filepath}')
        return filepath
