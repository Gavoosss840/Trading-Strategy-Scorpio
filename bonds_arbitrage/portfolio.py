"""
Portfolio Tracker — Open positions, P&L, persistence (JSON)
"""

import json
import os
import numpy as np
import logging
from datetime import datetime
from typing import Dict, List, Optional

from .config import PORTFOLIO_FILE

logger = logging.getLogger(__name__)


class PortfolioTracker:
    """
    Tracks open positions and closed trade history.
    Persists to JSON so state survives bot restarts.
    """

    def __init__(self, filepath: str = PORTFOLIO_FILE):
        self.filepath      = filepath
        self.positions:     Dict[str, Dict] = {}
        self.closed_trades: List[Dict]      = []
        self.load()

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath) as f:
                    data = json.load(f)
                self.positions     = data.get('positions', {})
                self.closed_trades = data.get('closed_trades', [])
                logger.info(f'[PORTFOLIO] Loaded: {len(self.positions)} open positions')
            except Exception as e:
                logger.warning(f'[PORTFOLIO] Load error: {e}')

    def save(self):
        try:
            with open(self.filepath, 'w') as f:
                json.dump({'positions': self.positions,
                           'closed_trades': self.closed_trades},
                          f, indent=2, default=str)
        except Exception as e:
            logger.warning(f'[PORTFOLIO] Save error: {e}')

    def open_position(self, trade: Dict, qty_long: int, qty_short: int,
                      tp_sl: Dict, ib_order_ids: Dict) -> str:
        pid = f"{trade['pair']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.positions[pid] = {
            'id':           pid,
            'pair':         trade['pair'],
            'signal':       trade['signal'],
            'z_entry':      trade['z_score'],
            'spread_entry': trade['spread_current'],
            'entry_time':   datetime.now().isoformat(),
            'leg_long': {
                'futures':     trade['leg_long']['futures'],
                'entry_price': trade['leg_long']['price'],
                'qty':         qty_long,
                'dv01':        trade['leg_long']['npv'].get('dv01', 0),
                'tp':          tp_sl.get('leg_long', {}).get('tp'),
                'sl':          tp_sl.get('leg_long', {}).get('sl'),
                'tp_order_id': ib_order_ids.get('tp_long'),
                'sl_order_id': ib_order_ids.get('sl_long'),
            },
            'leg_short': {
                'futures':     trade['leg_short']['futures'],
                'entry_price': trade['leg_short']['price'],
                'qty':         qty_short,
                'dv01':        trade['leg_short']['npv'].get('dv01', 0),
                'tp':          tp_sl.get('leg_short', {}).get('tp'),
                'sl':          tp_sl.get('leg_short', {}).get('sl'),
                'tp_order_id': ib_order_ids.get('tp_short'),
                'sl_order_id': ib_order_ids.get('sl_short'),
            },
            'status': 'open',
        }
        self.save()
        logger.info(f'[PORTFOLIO] Opened: {pid}')
        return pid

    def close_position(self, pid: str, exit_price_long: float,
                       exit_price_short: float, reason: str = '') -> Optional[float]:
        if pid not in self.positions:
            return None
        pos = self.positions.pop(pid)
        ll  = pos['leg_long']
        ls  = pos['leg_short']
        pnl_long  = (exit_price_long  - ll['entry_price']) * ll['qty'] * 1_000
        pnl_short = (ls['entry_price'] - exit_price_short) * ls['qty'] * 1_000
        pnl_total = pnl_long + pnl_short
        self.closed_trades.append({
            **pos,
            'exit_time':        datetime.now().isoformat(),
            'exit_price_long':  exit_price_long,
            'exit_price_short': exit_price_short,
            'pnl_long':         pnl_long,
            'pnl_short':        pnl_short,
            'pnl_total':        pnl_total,
            'close_reason':     reason,
            'status':           'closed',
        })
        self.save()
        logger.info(f'[PORTFOLIO] Closed: {pid}  reason={reason}  PnL=${pnl_total:+,.0f}')
        return pnl_total

    def update_tp_sl(self, pid: str, leg: str, tp: float, sl: float,
                     tp_order_id=None, sl_order_id=None):
        if pid in self.positions:
            self.positions[pid][leg]['tp'] = tp
            self.positions[pid][leg]['sl'] = sl
            if tp_order_id:
                self.positions[pid][leg]['tp_order_id'] = tp_order_id
            if sl_order_id:
                self.positions[pid][leg]['sl_order_id'] = sl_order_id
            self.save()

    # ── Metrics ───────────────────────────────────────────────────────

    def total_pnl(self) -> float:
        return sum(t.get('pnl_total', 0) for t in self.closed_trades)

    def win_rate(self) -> float:
        if not self.closed_trades:
            return 0.0
        wins = sum(1 for t in self.closed_trades if t.get('pnl_total', 0) > 0)
        return wins / len(self.closed_trades) * 100

    def max_drawdown(self) -> float:
        if not self.closed_trades:
            return 0.0
        equity = np.cumsum([t.get('pnl_total', 0) for t in self.closed_trades])
        peak   = np.maximum.accumulate(equity)
        return float((equity - peak).min())

    def sharpe(self) -> float:
        if len(self.closed_trades) < 2:
            return 0.0
        pnls = [t.get('pnl_total', 0) for t in self.closed_trades]
        mu   = np.mean(pnls)
        std  = np.std(pnls)
        return float(mu / std * np.sqrt(252)) if std > 0 else 0.0
