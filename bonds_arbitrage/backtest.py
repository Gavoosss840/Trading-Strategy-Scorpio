"""
Backtester — Walk-forward simulation on FRED/ECB historical data

Bugs fixed vs v1:
  1. Legs corrected in analytics.py (LONG A when spread A-B is wide)
  2. P&L sign corrected: SPREAD_LONG_A profits when spread widens (+d_sprd)
                         SPREAD_SHORT_A profits when spread narrows (-d_sprd)
  3. Per-position cumulative P&L (not last-day total)
  4. Sizing uses self.capital (fixed), not growing equity
  5. Realistic DV01 per contract in $
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
    Walk-forward backtest on FRED/ECB sovereign yield data.

    For each trading day (after the warm-up window):
      1. Compute rolling z-scores up to that date
      2. Check TP/SL on open positions
      3. Open new positions on fresh signals
      4. Estimate daily P&L via DV01 × Δspread

    P&L logic (corrected):
      Spread = yield_A − yield_B
      SPREAD_LONG_A  (LONG A / SHORT B): profit when spread widens  → +Δspread × DV01
      SPREAD_SHORT_A (LONG B / SHORT A): profit when spread narrows → −Δspread × DV01

    Output: equity curve, trade list, performance metrics.
    """

    # Realistic DV01 per contract in USD ($/bp/contract)
    # Based on approx modified duration and face value:
    #   ZT ($200k face, dur≈1.9): ~$38  | FGBS (€100k, dur≈1.9): ~$38
    #   ZF ($100k face, dur≈4.2): ~$42  | FGBM (€100k, dur≈4.2): ~$42
    #   ZN ($100k face, dur≈7.0): ~$70  | FGBL (€100k, dur≈7.0): ~$70
    #   ZB ($100k face, dur≈15):  ~$150 | FGBX (€100k, dur≈15):  ~$150
    DV01_BY_MAT: Dict[int, float] = {2: 38, 5: 42, 10: 70, 30: 150}

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

        for day in idx[ZSCORE_WINDOW:]:
            day_pnl = 0.0

            # ── 1. Signals for today ──────────────────────────────────
            signals = self.spread.analyze_all(until=day)
            sig_map = {
                (r['country_a'], r['country_b'], r['maturity']): r
                for r in signals
                if r.get('spread_current') is not None
            }

            # ── 2. Update / close open positions ─────────────────────
            still_open = []
            for pos in open_positions:
                ca, cb, mat = pos['country_a'], pos['country_b'], pos['maturity']
                key = (ca, cb, mat)
                sr  = sig_map.get(key)
                if sr is None:
                    still_open.append(pos)
                    continue

                # Daily spread change in basis points
                ss = self.spread.spread_series(ca, cb, mat, until=day)
                if ss is None or len(ss) < 2:
                    still_open.append(pos)
                    continue

                d_sprd = float(ss.iloc[-1] - ss.iloc[-2]) * 100  # % → bps

                z_now = sr['z_score']
                dv01  = self.DV01_BY_MAT.get(mat, 70)
                qty   = pos.get('qty', 1)

                # ── Correct P&L direction ─────────────────────────────
                # SPREAD_LONG_A  = LONG A / SHORT B
                #   profit when spread (yield_A - yield_B) widens → d_sprd > 0
                # SPREAD_SHORT_A = LONG B / SHORT A
                #   profit when spread narrows → d_sprd < 0
                if pos['signal'] == 'SPREAD_LONG_A':
                    pos_pnl = +d_sprd * dv01 * qty
                else:  # SPREAD_SHORT_A
                    pos_pnl = -d_sprd * dv01 * qty

                # Accumulate cumulative P&L per position
                pos['running_pnl'] = pos.get('running_pnl', 0.0) + pos_pnl
                day_pnl += pos_pnl

                # ── Exit conditions ───────────────────────────────────
                closed = False

                # TP: z has reverted toward 0
                if abs(z_now) < ZSCORE_EXIT:
                    pos.update({
                        'exit_reason': 'TP_ZSCORE',
                        'exit_date':   str(day.date()),
                        'pnl':         pos['running_pnl'],
                    })
                    all_trades.append(pos)
                    closed = True

                # SL: z extended beyond stop threshold
                elif pos['signal'] == 'SPREAD_LONG_A' and \
                        z_now < -(ZSCORE_ENTRY + SL_ZSCORE_EXTEND):
                    pos.update({
                        'exit_reason': 'SL',
                        'exit_date':   str(day.date()),
                        'pnl':         pos['running_pnl'],
                    })
                    all_trades.append(pos)
                    closed = True

                elif pos['signal'] == 'SPREAD_SHORT_A' and \
                        z_now > (ZSCORE_ENTRY + SL_ZSCORE_EXTEND):
                    pos.update({
                        'exit_reason': 'SL',
                        'exit_date':   str(day.date()),
                        'pnl':         pos['running_pnl'],
                    })
                    all_trades.append(pos)
                    closed = True

                if not closed:
                    still_open.append(pos)

            open_positions = still_open

            # ── 3. Open new positions ─────────────────────────────────
            open_keys = {(p['country_a'], p['country_b'], p['maturity'])
                         for p in open_positions}
            for sr in signals:
                if sr['signal'] in ('HOLD', 'NO_DATA'):
                    continue
                if sr.get('spread_current') is None:
                    continue
                key = (sr['country_a'], sr['country_b'], sr['maturity'])
                if key in open_keys or sr['confidence'] < MIN_CONFIDENCE:
                    continue

                # Fixed sizing: use initial capital (not growing equity)
                mat      = sr['maturity']
                dv01     = self.DV01_BY_MAT.get(mat, 70)
                # Qty: allocate 2% of initial capital, limited by contract face value
                # Face value approx: 2Y→$200k, others→$100k
                face     = 200_000 if mat == 2 else 100_000
                qty      = max(1, int(self.capital * 0.02 / face))

                open_positions.append({
                    **sr,
                    'entry_date':  str(day.date()),
                    'entry_z':     sr['z_score'],
                    'qty':         qty,
                    'running_pnl': 0.0,
                })
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
        sharpe   = float(arr.mean() / arr.std() * np.sqrt(252)) \
            if arr.std() > 0 else 0.0

        return {
            'date_from':      date_from,
            'date_to':        str(dt_to.date()),
            'n_days':         len(idx) - ZSCORE_WINDOW,
            'initial_cap':    self.capital,
            'final_equity':   equity,
            'total_return':   (equity - self.capital) / self.capital * 100,
            'sharpe':         sharpe,
            'max_drawdown':   max_dd,
            'max_dd_pct':     max_dd / self.capital * 100,
            'n_trades':       len(all_trades),
            'win_rate':       win_rate,
            'open_positions': len(open_positions),
            'equity_curve':   equity_curve,
            'daily_pnl':      daily_pnl,
            'trades':         all_trades,
        }
