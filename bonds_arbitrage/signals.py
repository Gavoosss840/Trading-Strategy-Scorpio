"""
Signals — Aggregates spread z-score signal with NPV/DCF confirmation.
Computes DV01-neutral leg sizing ratio.
"""

from typing import Dict, List, Optional, Tuple

from .config import NPV_THRESHOLD
from .analytics import BondPricer


class SignalAggregator:
    """
    Combines Signal 1 (spread z-score) with Signal 2 (NPV confirmation).

    NPV confirms  → confidence × 1.20
    NPV opposes   → confidence × 0.60
    NPV neutral   → confidence × 0.80

    Also computes DV01-neutral ratio: qty_short = qty_long × (DV01_long / DV01_short)
    """

    FUTURES_PARAMS: Dict[str, Tuple] = {
        # symbol: (country, coupon_approx, par, nominal_maturity_years)
        'ZT':   ('US', 0.045, 100,  2.0),
        'ZF':   ('US', 0.042, 100,  5.0),
        'ZN':   ('US', 0.043, 100, 10.0),
        'ZB':   ('US', 0.045, 100, 30.0),
        'FGBS': ('DE', 0.010, 100,  2.0),
        'FGBM': ('DE', 0.010, 100,  5.0),
        'FGBL': ('DE', 0.020, 100, 10.0),
        'FGBX': ('DE', 0.025, 100, 30.0),
        'R':    ('UK', 0.035, 100, 10.0),
    }
    COUNTRY_TO_FUTURES: Dict[Tuple, str] = {
        ('US',  2): 'ZT',  ('US',  5): 'ZF',
        ('US', 10): 'ZN',  ('US', 30): 'ZB',
        ('DE',  2): 'FGBS',('DE',  5): 'FGBM',
        ('DE', 10): 'FGBL',('DE', 30): 'FGBX',
        ('UK', 10): 'R',
    }
    FUTURES_EXCHANGE: Dict[str, str] = {
        'ZT': 'CBOT', 'ZF': 'CBOT', 'ZN': 'CBOT', 'ZB': 'CBOT',
        'FGBS': 'EUREX', 'FGBM': 'EUREX', 'FGBL': 'EUREX', 'FGBX': 'EUREX',
        'R': 'LIFFE',
    }

    def __init__(self, pricer: BondPricer):
        self.pricer = pricer

    def _npv_check(self, symbol: str, market_price: Optional[float],
                   leg_role: str) -> Dict:
        """Check NPV confirmation for one leg. leg_role: 'long' or 'short'."""
        if symbol not in self.FUTURES_PARAMS or market_price is None:
            return {'confirmed': False, 'alpha_pct': 0.0, 'multiplier': 0.80,
                    'mod_dur': 0, 'dv01': 0, 'fair_price': market_price or 100}

        country, coupon, par, years = self.FUTURES_PARAMS[symbol]
        npv = self.pricer.npv_alpha(country, coupon, par, years, market_price)
        a   = npv['alpha_pct']

        confirmed = (leg_role == 'long'  and a < -NPV_THRESHOLD) or \
                    (leg_role == 'short' and a >  NPV_THRESHOLD)
        opposed   = (leg_role == 'long'  and a >  NPV_THRESHOLD) or \
                    (leg_role == 'short' and a < -NPV_THRESHOLD)
        mult      = 1.20 if confirmed else (0.60 if opposed else 0.80)

        return {
            'confirmed':  confirmed,
            'alpha_pct':  a,
            'alpha_bps':  npv['alpha_bps'],
            'fair_price': npv['fair_price'],
            'mod_dur':    npv['modified_duration'],
            'convexity':  npv['convexity'],
            'dv01':       npv['dv01'],
            'multiplier': mult,
        }

    def build_trade(self, sr: Dict, prices: Dict[str, float]) -> Optional[Dict]:
        """Build a complete trade dict from a spread signal + current prices."""
        if sr['signal'] in ('HOLD', 'NO_DATA'):
            return None

        ca, cb, mat = sr['country_a'], sr['country_b'], sr['maturity']
        ll = cb if sr['signal'] == 'SPREAD_SHORT_A' else ca
        ls = ca if sr['signal'] == 'SPREAD_SHORT_A' else cb

        fl = self.COUNTRY_TO_FUTURES.get((ll, mat))
        fs = self.COUNTRY_TO_FUTURES.get((ls, mat))
        if not fl or not fs:
            return None

        pl  = prices.get(fl)
        ps  = prices.get(fs)
        nl  = self._npv_check(fl, pl, 'long')
        ns  = self._npv_check(fs, ps, 'short')

        # DV01-neutral ratio
        dv01_l     = nl['dv01'] if nl['dv01'] > 0 else 1
        dv01_s     = ns['dv01'] if ns['dv01'] > 0 else 1
        dv01_ratio = dv01_s / dv01_l

        mult = (nl['multiplier'] + ns['multiplier']) / 2
        conf = min(sr['confidence'] * mult, 100.0)

        return {
            'pair':           sr['pair'],
            'signal':         sr['signal'],
            'z_score':        sr['z_score'],
            'z_entry':        sr['z_score'],
            'spread_current': sr['spread_current'],
            'spread_mean':    sr['spread_mean'],
            'spread_std':     sr['spread_std'],
            'deviation_bps':  sr['deviation_bps'],
            'confidence':     conf,
            'dv01_ratio':     dv01_ratio,
            'leg_long':  {
                'futures':  fl,
                'exchange': self.FUTURES_EXCHANGE.get(fl, 'SMART'),
                'price':    pl,
                'npv':      nl,
            },
            'leg_short': {
                'futures':  fs,
                'exchange': self.FUTURES_EXCHANGE.get(fs, 'SMART'),
                'price':    ps,
                'npv':      ns,
            },
        }
