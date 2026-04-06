"""
CLI — Command page & entry point

All commands:
  python bonds.py help
  python bonds.py start [--paper] [--port PORT] [--dry-run false]
  python bonds.py backtest [--from DATE] [--to DATE] [--capital N] [--html]
  python bonds.py report [--html]
  python bonds.py positions
  python bonds.py spreads
"""

import argparse
import asyncio
import logging
import sys
from typing import Optional

from .config import IB_HOST, IB_PORT, BT_DEFAULT_FROM, BT_DEFAULT_CAPITAL
from .data import SovereignYieldFetcher
from .analytics import YieldCurveBuilder, BondPricer, SpreadAnalyzer
from .signals import SignalAggregator
from .risk import RiskManager, DynamicPositionSizer
from .portfolio import PortfolioTracker
from .backtest import Backtester
from .reports import ReportGenerator

logger = logging.getLogger(__name__)

# ==============================================================================
# PAGE DE COMMANDES
# ==============================================================================

HELP_TEXT = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║        S C O R P I O  —  Government Bond Arbitrage  v2.0                   ║
║        Spread Inter-pays  ×  NPV/DCF Confirmation                          ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  COMMANDES                                                                   ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  start          Lancer le bot en trading live                               ║
║    --paper        Paper trading  (défaut : port 7497)                       ║
║    --port PORT    Port IBKR  (7497=Paper  7496=Live)                        ║
║    --dry-run false  Activer les vrais ordres                                ║
║                                                                              ║
║  backtest       Simuler la stratégie sur données FRED historiques           ║
║    --from DATE    Date de début  (défaut : 2019-01-01)                      ║
║    --to   DATE    Date de fin    (défaut : aujourd'hui)                     ║
║    --capital N    Capital initial  (défaut : 1 000 000 $)                  ║
║    --html         Exporter le rapport en HTML  (output/backtest_report.html)║
║                                                                              ║
║  report         Rapport de performance (positions + métriques)              ║
║    --html         Exporter en HTML  (output/report.html)                    ║
║                                                                              ║
║  positions      Afficher les positions ouvertes                             ║
║                                                                              ║
║  spreads        Analyser les spreads actuels (sans IBKR)                   ║
║                                                                              ║
║  help           Afficher cette page                                         ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PÉRIODICITÉ                                                                 ║
║                                                                              ║
║  Bot en continu  →  gère lui-même le calendrier                             ║
║  Rebalancing     →  1× par jour  à 14h UTC  (10h ET / 15h Paris)           ║
║  Monitoring TP/SL→  toutes les 5 minutes  (automatique)                    ║
║  Refresh données →  toutes les 6 heures   (automatique)                    ║
║  Backtest        →  à la demande avant tout changement de paramètre        ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PAIRES SURVEILLÉES                                                          ║
║                                                                              ║
║  US10Y - DE10Y   (Fed / BCE — référence mondiale)                          ║
║  US2Y  - DE2Y    (Front de courbe Fed / BCE)                                ║
║  US5Y  - DE5Y                                                               ║
║  US10Y - UK10Y   (Fed / BoE)                                                ║
║  US10Y - JP10Y   (Fed / BoJ — YCC)                                         ║
║  US10Y - FR10Y   (Risque politique EU)                                      ║
║  DE10Y - IT10Y   (Spread périphérique EU)                                   ║
║  DE10Y - FR10Y   (Core EU)                                                  ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PARAMÈTRES CLÉS  (bonds_arbitrage/config.py)                               ║
║                                                                              ║
║  ZSCORE_ENTRY = 2.0   ZSCORE_EXIT = 0.5   ZSCORE_STOP = 3.5               ║
║  MIN_CONFIDENCE = 30%   POSITION = 1 – 5% du capital                       ║
║  TP_ZSCORE_TARGET = 0.3   SL_ZSCORE_EXTEND = 1.0                           ║
║  REBALANCE_HOUR = 14 UTC   MONITOR_INTERVAL = 300s                         ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""


# ==============================================================================
# HELPERS
# ==============================================================================

def _build_context(history_days: int = 400):
    """Build full analytics context without IBKR."""
    fetcher    = SovereignYieldFetcher(history_days=history_days)
    fetcher.fetch_all()
    curve      = YieldCurveBuilder(fetcher)
    curve.fit_all()
    pricer     = BondPricer(curve)
    spread_ana = SpreadAnalyzer(fetcher)
    aggregator = SignalAggregator(pricer)
    risk_mgr   = RiskManager()
    return fetcher, curve, pricer, spread_ana, aggregator, risk_mgr


# ==============================================================================
# COMMANDS
# ==============================================================================

def cmd_help():
    print(HELP_TEXT)


def cmd_spreads():
    print('\nFetching sovereign yields...')
    _, _, _, spread_ana, _, _ = _build_context()
    results = spread_ana.analyze_all()
    ReportGenerator().print_spreads(results)


def cmd_positions():
    portfolio = PortfolioTracker()
    reporter  = ReportGenerator()
    reporter.print_positions(portfolio)
    reporter.print_summary(portfolio)


def cmd_report(html: bool = False):
    fetcher, _, _, spread_ana, _, _ = _build_context()
    results   = spread_ana.analyze_all()
    portfolio = PortfolioTracker()
    reporter  = ReportGenerator()
    reporter.print_spreads(results)
    reporter.print_positions(portfolio)
    reporter.print_summary(portfolio)
    if html:
        path = reporter.save_html(portfolio, results)
        print(f'\n-> HTML report: {path}')


def cmd_backtest(date_from: str, date_to: Optional[str],
                 capital: float, html: bool):
    dt_label = date_to or "today"
    print(f'\nBacktest {date_from} -> {dt_label}   capital=${capital:,.0f}')
    fetcher, _, pricer, spread_ana, aggregator, risk_mgr = _build_context(history_days=2000)
    bt      = Backtester(fetcher, spread_ana, aggregator, risk_mgr, capital)
    metrics = bt.run(date_from, date_to)
    if not metrics:
        print('Backtest failed — insufficient historical data.')
        return
    reporter = ReportGenerator()
    reporter.print_backtest(metrics)
    if html:
        portfolio = PortfolioTracker()
        path = reporter.save_html(portfolio, [], backtest=metrics,
                                  filepath='backtest_report.html')
        print(f'\n-> HTML report: {path}')


def cmd_start(port: int, dry_run: bool):
    import bonds_arbitrage.config as cfg
    cfg.DRY_RUN = dry_run

    from .bot import GovernmentBondsBot

    bot = GovernmentBondsBot(ib_host=IB_HOST, ib_port=port)

    async def _run():
        if not await bot.connect():
            print('[ERROR] Cannot connect to IBKR TWS.')
            print('  1. Is TWS open?')
            print('  2. API enabled? -> File -> Global Config -> API -> Enable')
            print(f'  3. Port: {port}  (7497=Paper | 7496=Live)')
            return
        try:
            await bot.run()
        except KeyboardInterrupt:
            bot.running = False
            print('\n[SHUTDOWN] Ctrl+C')

    asyncio.run(_run())


# ==============================================================================
# ARGPARSE MAIN
# ==============================================================================

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bonds_arbitrage.log', encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )

    parser = argparse.ArgumentParser(
        prog='bonds',
        description='Scorpio — Government Bond Arbitrage',
        add_help=False,
    )
    sub = parser.add_subparsers(dest='command')

    # start
    p_start = sub.add_parser('start')
    p_start.add_argument('--paper',    action='store_true', default=True)
    p_start.add_argument('--port',     type=int, default=IB_PORT)
    p_start.add_argument('--dry-run',
                         type=lambda x: x.lower() != 'false',
                         default=True, dest='dry_run')

    # backtest
    p_bt = sub.add_parser('backtest')
    p_bt.add_argument('--from',    dest='date_from', default=BT_DEFAULT_FROM)
    p_bt.add_argument('--to',      dest='date_to',   default=None)
    p_bt.add_argument('--capital', type=float,       default=BT_DEFAULT_CAPITAL)
    p_bt.add_argument('--html',    action='store_true')

    # report
    p_rep = sub.add_parser('report')
    p_rep.add_argument('--html', action='store_true')

    # other
    sub.add_parser('positions')
    sub.add_parser('spreads')
    sub.add_parser('help')

    args = parser.parse_args()

    dispatch = {
        'start':     lambda: cmd_start(args.port, args.dry_run),
        'backtest':  lambda: cmd_backtest(args.date_from, args.date_to,
                                          args.capital, args.html),
        'report':    lambda: cmd_report(args.html),
        'positions': cmd_positions,
        'spreads':   cmd_spreads,
        'help':      cmd_help,
    }

    handler = dispatch.get(args.command)
    if handler:
        handler()
    else:
        cmd_help()
