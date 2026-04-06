"""
Risk Management — TP/SL (DV01-based) and Position Sizing
"""

from typing import Dict, Optional

from .config import (
    TP_ZSCORE_TARGET, SL_ZSCORE_EXTEND, TP_SL_UPDATE_THRESHOLD,
    ZSCORE_ENTRY, MIN_CONFIDENCE, MIN_POSITION_PCT, MAX_POSITION_PCT,
)


class RiskManager:
    """
    Computes TP and SL price levels for each leg of a spread trade.

    Method: DV01-based conversion
      TP bps = (|z_entry| - TP_ZSCORE_TARGET) × spread_std × 100
      SL bps = SL_ZSCORE_EXTEND × spread_std × 100
      price_TP = entry ± DV01 × bps_TP
      price_SL = entry ∓ DV01 × bps_SL
    """

    def compute_tp_sl(self, trade: Dict) -> Dict:
        """Returns {'leg_long': {tp, sl, entry, dv01, bps_tp, bps_sl}, 'leg_short': {...}}"""
        z_entry = abs(trade.get('z_entry', ZSCORE_ENTRY))
        std     = trade.get('spread_std', 0.20) or 0.20

        bps_tp = (z_entry - TP_ZSCORE_TARGET) * std * 100
        bps_sl = SL_ZSCORE_EXTEND * std * 100

        result = {}
        for role, leg_key in (('long', 'leg_long'), ('short', 'leg_short')):
            leg   = trade.get(leg_key, {})
            entry = leg.get('price')
            dv01  = leg.get('npv', {}).get('dv01', 0)

            if entry is None or dv01 == 0:
                result[leg_key] = {'tp': None, 'sl': None, 'entry': entry}
                continue

            if role == 'long':
                tp = entry + dv01 * bps_tp
                sl = entry - dv01 * bps_sl
            else:
                tp = entry - dv01 * bps_tp
                sl = entry + dv01 * bps_sl

            result[leg_key] = {
                'tp':     round(tp, 4),
                'sl':     round(sl, 4),
                'entry':  entry,
                'dv01':   dv01,
                'bps_tp': bps_tp,
                'bps_sl': bps_sl,
            }
        return result

    def should_update(self, old_tp: Optional[float], new_tp: Optional[float],
                      current_price: Optional[float]) -> bool:
        """True if TP/SL should be recalculated (price moved > TP_SL_UPDATE_THRESHOLD)."""
        if old_tp is None or new_tp is None or current_price is None:
            return True
        return abs(new_tp - old_tp) / current_price > TP_SL_UPDATE_THRESHOLD


class DynamicPositionSizer:
    """
    Linear interpolation between min and max position sizes based on confidence.
    Returns DV01-neutral quantities for both legs.
    """

    def __init__(self, min_pct: float = MIN_POSITION_PCT,
                 max_pct: float = MAX_POSITION_PCT,
                 min_conf: float = MIN_CONFIDENCE):
        self.min_pct  = min_pct
        self.max_pct  = max_pct
        self.min_conf = min_conf

    def calculate(self, equity: float, confidence: float, price: float,
                  dv01_ratio: float = 1.0) -> Dict:
        if confidence < self.min_conf or equity <= 0 or price <= 0:
            return {'can_trade': False, 'reason': f'conf={confidence:.0f}%',
                    'qty_long': 0, 'qty_short': 0}

        norm    = max(0.0, min(1.0, (confidence - self.min_conf) / (100.0 - self.min_conf)))
        pct     = self.min_pct + (self.max_pct - self.min_pct) * norm
        value   = equity * pct / 100.0
        q_long  = max(1, int(value / (price * 1_000)))
        q_short = max(1, int(q_long * dv01_ratio))

        return {
            'can_trade': True,
            'qty_long':  q_long,
            'qty_short': q_short,
            'pct':       pct,
            'value':     q_long * price * 1_000,
        }
