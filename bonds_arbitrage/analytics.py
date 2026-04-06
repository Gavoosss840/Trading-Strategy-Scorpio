"""
Analytics — Yield Curve, Bond Pricer, Spread Analyzer
"""

import logging
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from typing import Dict, List, Optional, Tuple

from .config import (ZSCORE_WINDOW, ZSCORE_STD_WIN, ZSCORE_ENTRY, ZSCORE_EXIT,
                     ZSCORE_WINDOW_MONTHLY, ZSCORE_STD_WIN_MONTHLY, SPREAD_PAIRS)
from .data import SovereignYieldFetcher

logger = logging.getLogger(__name__)


# ==============================================================================
# YIELD CURVE
# ==============================================================================

class YieldCurveBuilder:
    """Nelson-Siegel yield curve fitted per country."""

    def __init__(self, fetcher: SovereignYieldFetcher):
        self.fetcher = fetcher
        self._params: Dict[str, Tuple] = {}

    @staticmethod
    def _ns(t, b0, b1, b2, tau):
        if t <= 0:
            return b0 + b1
        d = (1 - np.exp(-t / tau)) / (t / tau)
        return b0 + b1 * d + b2 * (d - np.exp(-t / tau))

    def fit(self, country: str, until: Optional[pd.Timestamp] = None):
        points = {}
        for mat in (1, 2, 5, 10, 30):
            if until is not None:
                s = self.fetcher.history_until(country, mat, until)
                v = float(s.iloc[-1]) if s is not None and len(s) > 0 else None
            else:
                v = self.fetcher.latest(country, mat)
            if v is not None:
                points[mat] = v

        if len(points) < 2:
            self._params[country] = (4.0, -0.5, 1.0, 2.0)
            return

        mats = np.array(list(points.keys()), dtype=float)
        obs  = np.array(list(points.values()), dtype=float)

        def obj(p):
            b0, b1, b2, tau = p
            if tau <= 0 or b0 <= 0:
                return 1e10
            return np.sum(
                (np.array([self._ns(t, b0, b1, b2, tau) for t in mats]) - obs) ** 2
            )

        res = minimize(obj, [obs.mean(), obs[0] - obs[-1], 0.5, 2.0],
                       bounds=[(0.01, 20), (-15, 15), (-15, 15), (0.1, 30)],
                       method='L-BFGS-B')
        self._params[country] = tuple(res.x) if res.success else \
            (obs.mean(), -0.3, 0.5, 2.0)

    def fit_all(self, until: Optional[pd.Timestamp] = None):
        for c in ('US', 'DE', 'FR', 'IT', 'UK', 'JP'):
            self.fit(c, until)

    def get_rate(self, country: str, maturity_years: float) -> float:
        """Yield (%) via Nelson-Siegel for a given country and maturity."""
        if country not in self._params:
            self.fit(country)
        return self._ns(maturity_years, *self._params[country])


# ==============================================================================
# BOND PRICER
# ==============================================================================

class BondPricer:
    """Full DCF bond pricer — price, modified duration, convexity, DV01."""

    def __init__(self, curve: YieldCurveBuilder):
        self.curve = curve

    def price_bond(self, country: str, coupon_rate: float, par: float,
                   years: float, freq: int = 2) -> Dict:
        pmt = par * coupon_rate / freq
        n   = max(1, int(round(years * freq)))
        pv = dur = conv = 0.0

        for i in range(1, n + 1):
            t  = i / freq
            rf = self.curve.get_rate(country, t) / 100
            df = 1 / (1 + rf / freq) ** i
            cf = pmt + (par if i == n else 0)
            pv_cf = cf * df
            pv   += pv_cf
            dur  += t * pv_cf
            conv += t * (t + 1 / freq) * pv_cf

        if pv == 0:
            return {'price': par, 'macaulay_duration': 0,
                    'modified_duration': 0, 'convexity': 0, 'dv01': 0}

        mid_rf  = self.curve.get_rate(country, years / 2) / 100
        mac_dur = dur / pv
        mod_dur = mac_dur / (1 + mid_rf / freq)
        convex  = conv / (pv * (1 + mid_rf / freq) ** 2)
        return {
            'price':             pv,
            'macaulay_duration': mac_dur,
            'modified_duration': mod_dur,
            'convexity':         convex,
            'dv01':              mod_dur * pv * 0.0001,
        }

    def npv_alpha(self, country: str, coupon_rate: float, par: float,
                  years: float, market_price: float) -> Dict:
        """Alpha = (market_price - fair_price) / fair_price in %."""
        fair      = self.price_bond(country, coupon_rate, par, years)
        alpha     = market_price - fair['price']
        alpha_pct = alpha / fair['price'] * 100
        return {
            'fair_price': fair['price'],
            'alpha':      alpha,
            'alpha_pct':  alpha_pct,
            'alpha_bps':  alpha / fair['price'] * 10_000,
            **fair,
        }


# ==============================================================================
# SPREAD ANALYZER
# ==============================================================================

class SpreadAnalyzer:
    """
    Computes inter-country yield spreads and z-scores.

    Signal logic (corrected):
      Spread = yield_A − yield_B
      z > +ZSCORE_ENTRY  →  spread too wide   →  LONG A bonds / SHORT B bonds
                              (A cheap, B expensive relative to historical spread)
      z < −ZSCORE_ENTRY  →  spread too tight  →  SHORT A bonds / LONG B bonds
    """

    def __init__(self, fetcher: SovereignYieldFetcher,
                 window: int = ZSCORE_WINDOW,
                 std_window: int = ZSCORE_STD_WIN,
                 z_entry: float = ZSCORE_ENTRY,
                 z_exit: float = ZSCORE_EXIT):
        self.fetcher        = fetcher
        self.window         = window      # mean window for daily data (trading days)
        self.std_window     = std_window  # std window for daily data
        self.window_m       = ZSCORE_WINDOW_MONTHLY    # mean window for monthly data
        self.std_window_m   = ZSCORE_STD_WIN_MONTHLY   # std window for monthly data
        self.z_entry        = z_entry
        self.z_exit         = z_exit

    def spread_series(self, ca: str, cb: str, mat: int,
                      until: Optional[pd.Timestamp] = None) -> Optional[pd.Series]:
        if until is not None:
            sa = self.fetcher.history_until(ca, mat, until)
            sb = self.fetcher.history_until(cb, mat, until)
        else:
            sa = self.fetcher.history(ca, mat)
            sb = self.fetcher.history(cb, mat)
        if sa is None or sb is None:
            return None
        df = pd.DataFrame({'a': sa, 'b': sb}).dropna()
        return (df['a'] - df['b']) if len(df) >= 20 else None

    def zscore(self, series: pd.Series) -> float:
        """
        Split-window z-score avec détection automatique de fréquence.

        Données journalières (ECB daily, ex: US-DE):
          mean  = last self.window  jours (252 = 1 an)
          sigma = last self.std_window jours (60 = 3 mois)

        Données mensuelles (FRED, ex: US-UK/JP/FR/IT):
          mean  = last 48 mois (4 ans) — évite les 20+ ans qui noient le signal
          sigma = last 12 mois (1 an) — volatilité récente

        → pour BTP-Bund à 250bps en 2022 vs moy 4 ans ~150bps, std 1 an ~50bps
          z = (250-150)/50 = 2.0  →  signal fort, vs z~1.1 avec 21 ans de moyenne
        """
        s = series.dropna()
        if len(s) < 10:
            return 0.0

        # Détecter la fréquence : mensuel si écart médian >= 20 jours
        if len(s) >= 3:
            gaps = s.index.to_series().diff().dt.days.dropna()
            is_monthly = gaps.median() >= 20
        else:
            is_monthly = False

        if is_monthly:
            w_mean = min(self.window_m,     len(s))
            w_std  = min(self.std_window_m, len(s))
        else:
            w_mean = min(self.window,     len(s))
            w_std  = min(self.std_window, len(s))

        mu    = float(s.iloc[-w_mean:].mean())
        sigma = float(s.iloc[-w_std:].std())
        # Floor: at least 10bps std to prevent z-score blow-up in stable regimes
        sigma = max(sigma, 0.10)
        return float((s.iloc[-1] - mu) / sigma)

    def analyze_pair(self, ca: str, cb: str, mat: int,
                     until: Optional[pd.Timestamp] = None) -> Dict:
        ss    = self.spread_series(ca, cb, mat, until)
        label = f'{ca}{mat}Y-{cb}{mat}Y'

        if ss is None or len(ss) < 10:
            return {
                'pair': label, 'country_a': ca, 'country_b': cb, 'maturity': mat,
                'signal': 'NO_DATA', 'z_score': 0.0,
                'spread_current': None, 'spread_mean': None, 'spread_std': None,
                'confidence': 0.0, 'leg_long': None, 'leg_short': None, 'n_points': 0,
            }

        z       = self.zscore(ss)
        w       = min(self.window, len(ss))
        mu      = float(ss.iloc[-w:].mean())
        sigma   = float(ss.iloc[-w:].std())
        current = float(ss.iloc[-1])

        # ── Signal (corrected legs) ───────────────────────────────────
        # Spread = yield_A - yield_B
        # z > 0: A is wide (cheap) relative to B → BUY A bonds, SELL B bonds
        # z < 0: A is tight (expensive) → SELL A bonds, BUY B bonds
        if z > self.z_entry:
            signal, ll, ls = 'SPREAD_LONG_A',  ca, cb   # LONG A (cheap) / SHORT B
        elif z < -self.z_entry:
            signal, ll, ls = 'SPREAD_SHORT_A', cb, ca   # LONG B (cheap) / SHORT A
        else:
            signal, ll, ls = 'HOLD', None, None

        z_pts = min(abs(z) / 3.0 * 50, 50.0)         # 50 pts at z=3
        h_pts = min(len(ss) / self.window * 50, 50.0)  # 50 pts at full mean window

        result = {
            'pair':           label,
            'country_a':      ca,
            'country_b':      cb,
            'maturity':       mat,
            'signal':         signal,
            'z_score':        z,
            'spread_current': current,
            'spread_mean':    mu,
            'spread_std':     sigma,
            'deviation_bps':  (current - mu) * 100,
            'confidence':     z_pts + h_pts,
            'leg_long':       ll,
            'leg_short':      ls,
            'n_points':       len(ss),
        }

        if signal not in ('HOLD', 'NO_DATA'):
            logger.info(
                f'[SPREAD] {label:16s}  z={z:+5.2f}  '
                f'spread={current:+6.3f}%  (moy={mu:+5.3f}% ±{sigma:.3f}%)  '
                f'dev={result["deviation_bps"]:+5.1f}bps  '
                f'{signal:20s}  conf={result["confidence"]:.0f}%'
            )
        return result

    def analyze_all(self, until: Optional[pd.Timestamp] = None) -> List[Dict]:
        return [self.analyze_pair(ca, cb, mat, until) for ca, cb, mat in SPREAD_PAIRS]
