"""
Backtester — Walk-forward simulation on FRED historical data
"""

import numpy as np
import pandas as pd
import logging
from typing import Dict, List, Optional

from .config import (
    ZSCORE_WINDOW, ZSCORE_ENTRY, ZSCORE_EXIT, SL_ZSCORE_EXTEND,
    MIN_CONFIDENCE, BT_DEFAULT_FROM, BT_DEFAULT_CAPITAL, SPREAD_PAIRS,
)
from .data import SovereignYieldFetcher
from .analytics import SpreadAnalyzer
from .signals import SignalAggregator
from .risk import RiskManager

logger = logging.getLogger(__name__)


class Backtester:
    """
    Walk-forward backtest on FRED sovereign yield data.

    For each trading day (after the warm-up window):
      1. Compute rolling z-scores up to that date
      2. Check TP/SL on open positions
      3. Open new positions on fresh signals
      4. Estimate daily P&L via DV01 × Δspread

    Output: equity curve, trade list, performance metrics.
    """

    def __init__(self, fetcher: SovereignYieldFetcher,
                 spread_ana: SpreadAnalyzer,
                 aggregator: SignalAggregator,
                 risk_mgr: RiskManager,
                 initial_capital: float = BT_DEFAULT_CAPITAL):
        self.fetcher   = fetcher
        self.spread    = spread_ana
        self.agg       = aggregator
        self.risk      = risk_mgr
        self.capital   = initial_capital

    def run(self, date_from: str = BT_DEFAULT_FROM,
            date_to: Optional[str] = None) -> Dict:

        dt_from = pd.Timestamp(date_from)
        dt_to   = pd.Timestamp(date_to) if date_to else pd.Timestamp.today()

        us10 = self.fetcher.history('US', 10)
        if us10 is None:
            logger.error('[BACKTEST] US 10Y data unavailable')
            return {}

        idx = us10[(us10.index >= dt_from) & (us10.index <= dt_to)].index
        if len(idx) < ZSCORE_WINDOW + 20:
            logger.error(f'[BACKTEST] Insufficient history ({len(idx)} days)')
            return {}

        logger.info(f'[BACKTEST] {date_from} → {dt_to.date()}  ({len(idx)} days)')

        equity         = self.capital
        equity_curve   = [equity]
        daily_pnl      = []
        open_positions: List[Dict] = []
        all_trades:     List[Dict] = []

        # DV01 approximation by maturity (per contract, in $)
        DV01_BY_MAT = {2: 200, 5: 450, 10: 850, 30: 1_800}

        for day in idx[ZSCORE_WINDOW:]:
            day_pnl = 0.0

            # ── 1. Signals ────────────────────────────────────────────
            signals  = self.spread.analyze_all(until=day)
            sig_map  = {(r['country_a'], r['country_b'], r['maturity']): r
                        for r in signals}

            # ── 2. Update / close open positions ─────────────────────
            still_open = []
            for pos in open_positions:
                key = (pos['country_a'], pos['country_b'], pos['maturity'])
                sr  = sig_map.get(key)
                if sr is None or sr['spread_series_last2'] is None:
                    still_open.append(pos)
                    continue

                z_now  = sr['z_score']
                d_sprd = sr.get('spread_delta_bps', 0.0)
                dv01   = DV01_BY_MAT.get(pos['maturity'], 850)
                qty    = pos.get('qty', 1)

                if pos['signal'] == 'SPREAD_LONG_A':
                    day_pnl -= d_sprd * dv01 * qty
                else:
                    day_pnl += d_sprd * dv01 * qty

                # TP
                closed = False
                if abs(z_now) < ZSCORE_EXIT:
                    pos.update({'exit_reason': 'TP_ZSCORE', 'exit_date': str(day.date()),
                                'pnl': day_pnl})
                    all_trades.append(pos); closed = True
                # SL
                elif (pos['signal'] == 'SPREAD_LONG_A'  and
                      z_now < -(ZSCORE_ENTRY + SL_ZSCORE_EXTEND)):
                    pos.update({'exit_reason': 'SL', 'exit_date': str(day.date())})
                    all_trades.append(pos); closed = True
                elif (pos['signal'] == 'SPREAD_SHORT_A' and
                      z_now >  (ZSCORE_ENTRY + SL_ZSCORE_EXTEND)):
                    pos.update({'exit_reason': 'SL', 'exit_date': str(day.date())})
                    all_trades.append(pos); closed = True

                if not closed:
                    still_open.append(pos)

            open_positions = still_open

            # ── 3. Open new positions ─────────────────────────────────
            open_keys = {(p['country_a'], p['country_b'], p['maturity'])
                         for p in open_positions}
            for sr in signals:
                if sr['signal'] in ('HOLD', 'NO_DATA'):
                    continue
                key = (sr['country_a'], sr['country_b'], sr['maturity'])
                if key in open_keys or sr['confidence'] < MIN_CONFIDENCE:
                    continue
                qty = max(1, int(equity * 0.02 /
                                 (DV01_BY_MAT.get(sr['maturity'], 850) * 10)))
                open_positions.append({**sr, 'entry_date': str(day.date()),
                                       'entry_z': sr['z_score'], 'qty': qty})
                open_keys.add(key)

            equity       += day_pnl
            daily_pnl.append(day_pnl)
            equity_curve.append(equity)

        # ── Metrics ───────────────────────────────────────────────────
        arr      = np.array(daily_pnl)
        eq       = np.array(equity_curve)
        peak     = np.maximum.accumulate(eq)
        max_dd   = float((eq - peak).min())
        wins     = sum(1 for t in all_trades if t.get('pnl', 0) > 0)
        win_rate = wins / len(all_trades) * 100 if all_trades else 0.0
        sharpe   = float(arr.mean() / arr.std() * np.sqrt(252)) if arr.std() > 0 else 0.0

        return {
            'date_from':     date_from,
            'date_to':       str(dt_to.date()),
            'n_days':        len(idx) - ZSCORE_WINDOW,
            'initial_cap':   self.capital,
            'final_equity':  equity,
            'total_return':  (equity - self.capital) / self.capital * 100,
            'sharpe':        sharpe,
            'max_drawdown':  max_dd,
            'max_dd_pct':    max_dd / self.capital * 100,
            'n_trades':      len(all_trades),
            'win_rate':      win_rate,
            'open_positions':len(open_positions),
            'equity_curve':  equity_curve,
            'daily_pnl':     daily_pnl,
            'trades':        all_trades,
        }
