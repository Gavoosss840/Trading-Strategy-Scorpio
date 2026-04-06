"""
Reports — Terminal (ASCII) and HTML report generation
All HTML files are saved in the output/ folder.
"""

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
    • HTML     : report.html with styled tables
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
            print(f'\n  {"TRADE":<22} {"ENTRÉE":<12} {"SORTIE":<12} {"RAISON":<15} {"P&L":>10}')
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
        # Always save inside output/
        filepath = os.path.join(OUTPUT_DIR, os.path.basename(filepath))
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        spread_rows = ''
        for r in spread_results:
            if r['spread_current'] is None:
                continue
            color = '#e74c3c' if abs(r['z_score']) > ZSCORE_ENTRY else '#2ecc71'
            spread_rows += (
                f'<tr>'
                f'<td>{r["pair"]}</td>'
                f'<td style="color:{color};font-weight:bold">{r["z_score"]:+.2f}</td>'
                f'<td>{r["spread_current"]:+.3f}%</td>'
                f'<td>{r["spread_mean"]:+.3f}%</td>'
                f'<td>{r["deviation_bps"]:+.1f}</td>'
                f'<td style="color:{color}">{r["signal"]}</td>'
                f'<td>{r["confidence"]:.0f}%</td>'
                f'</tr>\n'
            )

        pos_rows = ''
        for pid, pos in portfolio.positions.items():
            ll, ls = pos['leg_long'], pos['leg_short']
            pos_rows += (
                f'<tr><td>{pos["pair"]}</td><td>{pos["entry_time"][:16]}</td>'
                f'<td>{pos["z_entry"]:+.2f}</td>'
                f'<td>LONG {ll["futures"]} / SHORT {ls["futures"]}</td>'
                f'<td>{ll["tp"]} / {ls["tp"]}</td>'
                f'<td>{ll["sl"]} / {ls["sl"]}</td></tr>\n'
            )

        perf_html = ''
        if portfolio.closed_trades:
            perf_html = f'''
            <h2>Performance Live</h2>
            <table><tr><th>Métrique</th><th>Valeur</th></tr>
            <tr><td>P&L Total</td><td>${portfolio.total_pnl():+,.0f}</td></tr>
            <tr><td>Trades fermés</td><td>{len(portfolio.closed_trades)}</td></tr>
            <tr><td>Win Rate</td><td>{portfolio.win_rate():.1f}%</td></tr>
            <tr><td>Max Drawdown</td><td>${portfolio.max_drawdown():+,.0f}</td></tr>
            <tr><td>Sharpe</td><td>{portfolio.sharpe():.2f}</td></tr>
            </table>'''

        bt_html = ''
        if backtest:
            bt_html = f'''
            <h2>Backtest {backtest["date_from"]} -> {backtest["date_to"]}</h2>
            <table><tr><th>Métrique</th><th>Valeur</th></tr>
            <tr><td>Rendement</td><td>{backtest["total_return"]:+.2f}%</td></tr>
            <tr><td>Sharpe</td><td>{backtest["sharpe"]:.2f}</td></tr>
            <tr><td>Max Drawdown</td><td>{backtest["max_dd_pct"]:+.2f}%</td></tr>
            <tr><td>Win Rate</td><td>{backtest["win_rate"]:.1f}%</td></tr>
            <tr><td>Trades</td><td>{backtest["n_trades"]}</td></tr>
            </table>'''

        html = f'''<!DOCTYPE html><html lang="fr"><head>
        <meta charset="utf-8">
        <title>Scorpio Bond Arb — {now}</title>
        <style>
          body{{font-family:monospace;background:#0d1117;color:#c9d1d9;padding:2rem}}
          h1{{color:#58a6ff;border-bottom:1px solid #30363d;padding-bottom:.5rem}}
          h2{{color:#79c0ff;margin-top:2rem}}
          table{{border-collapse:collapse;width:100%;margin-top:.75rem}}
          th{{background:#161b22;padding:8px 12px;text-align:left;color:#58a6ff;
              border:1px solid #30363d}}
          td{{padding:6px 12px;border:1px solid #21262d}}
          tr:hover td{{background:#161b22}}
          .tag{{font-size:.75rem;padding:2px 6px;border-radius:3px;
                background:#1f6feb;color:#fff}}
        </style></head><body>
        <h1>Scorpio — Government Bond Arbitrage</h1>
        <p>Rapport généré le <strong>{now}</strong></p>

        <h2>Spreads Inter-pays</h2>
        <table>
          <tr><th>Paire</th><th>z-score</th><th>Spread</th><th>Moyenne</th>
              <th>Déviation (bps)</th><th>Signal</th><th>Conf</th></tr>
          {spread_rows}
        </table>

        <h2>Positions Ouvertes ({len(portfolio.positions)})</h2>
        <table>
          <tr><th>Paire</th><th>Entrée</th><th>z entrée</th>
              <th>Legs</th><th>TP</th><th>SL</th></tr>
          {pos_rows if pos_rows else "<tr><td colspan='6'>Aucune position</td></tr>"}
        </table>

        {perf_html}
        {bt_html}
        </body></html>'''

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info(f'[REPORT] Saved -> {filepath}')
        return filepath
