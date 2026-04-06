"""
================================================================================
Government Bond Arbitrage Strategy  —  v2.0
================================================================================

DEUX SOURCES D'ALPHA :
  Signal 1 — Spread inter-pays   (z-score sur fenêtre glissante)
  Signal 2 — NPV/DCF             (confirmation d'entrée + timing)

COMMANDES DISPONIBLES :
  python bonds_arbitrage_strategy.py help
  python bonds_arbitrage_strategy.py start [--paper] [--port PORT]
  python bonds_arbitrage_strategy.py backtest [--from YYYY-MM-DD] [--to YYYY-MM-DD]
                                              [--capital FLOAT] [--html]
  python bonds_arbitrage_strategy.py report [--html]
  python bonds_arbitrage_strategy.py positions
  python bonds_arbitrage_strategy.py spreads

PÉRIODICITÉ RECOMMANDÉE :
  • Lancer le bot en continu (run) — il gère lui-même les horaires
  • Rebalancing des signaux  : 1× par jour  (08h00 ET / 14h00 CET)
  • Monitoring TP/SL         : toutes les 5 min (en continu dans le bot)
  • Refresh données FRED/ECB : toutes les 6h

================================================================================
"""

# ==============================================================================
# CONFIGURATION
# ==============================================================================

IB_HOST          = '127.0.0.1'
IB_PORT          = 7497          # 7497 = Paper, 7496 = Live

# Z-score
ZSCORE_WINDOW    = 252           # jours glissants pour le z-score
ZSCORE_ENTRY     = 2.0           # entrée quand |z| > 2.0
ZSCORE_EXIT      = 0.5           # sortie quand |z| < 0.5
ZSCORE_STOP      = 3.5           # stop-loss spread si z franchit 3.5

# NPV
NPV_THRESHOLD    = 0.05          # alpha NPV minimum (%) pour confirmation

# Sizing
MIN_CONFIDENCE   = 30.0
MIN_POSITION_PCT = 1.0
MAX_POSITION_PCT = 5.0

# TP / SL (en multiples de l'écart-type du spread)
TP_ZSCORE_TARGET = 0.3           # TP quand z revient à ±0.3
SL_ZSCORE_EXTEND = 1.0           # SL si z s'étend de +1.0 au-delà de l'entrée
TP_SL_UPDATE_THRESHOLD = 0.15    # recalculer TP/SL si le prix a bougé de >0.15%

# Timing
REBALANCE_HOUR   = 14            # heure UTC du rebalancing quotidien (14h = 10h ET)
MONITOR_INTERVAL = 300           # secondes entre checks TP/SL (5 min)
DATA_REFRESH_H   = 6             # heures entre refresh des données souveraines

# Backtest
BT_DEFAULT_FROM    = '2019-01-01'
BT_DEFAULT_CAPITAL = 1_000_000.0

DRY_RUN          = True

# Paires surveillées : (pays_A, pays_B, maturité_années)
SPREAD_PAIRS = [
    ('US', 'DE', 10),
    ('US', 'DE',  2),
    ('US', 'DE',  5),
    ('US', 'UK', 10),
    ('US', 'JP', 10),
    ('US', 'FR', 10),
    ('DE', 'IT', 10),
    ('DE', 'FR', 10),
]

# ==============================================================================
# IMPORTS
# ==============================================================================

import numpy as np
import pandas as pd
import requests
import json
import os
import argparse
import sys
from scipy.optimize import minimize
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import asyncio
import logging

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bonds_arbitrage.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# SOVEREIGN YIELD FETCHER
# ==============================================================================

class SovereignYieldFetcher:
    FRED_US: Dict[int, str] = {1:'DGS1', 2:'DGS2', 5:'DGS5', 10:'DGS10', 30:'DGS30'}
    FRED_INTL: Dict[str, Dict[int, str]] = {
        'DE': {2:'IRLTST01DEM156N', 10:'IRLTLT01DEM156N'},
        'FR': {10:'IRLTLT01FRM156N'},
        'IT': {10:'IRLTLT01ITM156N'},
        'UK': {10:'IRLTLT01GBM156N'},
        'JP': {10:'IRLTLT01JPM156N'},
    }
    ECB_SERIES: Dict[str, Dict[int, str]] = {
        'DE': {
            2:  'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y',
            5:  'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y',
            10: 'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y',
        },
    }

    def __init__(self, history_days: int = 400):
        self.history_days = history_days
        self._cache: Dict[str, Dict[int, pd.Series]] = {}
        self.last_update: Optional[datetime] = None

    def _fred(self, sid: str) -> Optional[pd.Series]:
        start = (datetime.now() - timedelta(days=self.history_days)).strftime('%Y-%m-%d')
        try:
            r = requests.get(
                f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd={start}',
                timeout=8
            )
            if r.status_code != 200:
                return None
            records = {}
            for line in r.text.strip().split('\n')[1:]:
                p = line.split(',')
                if len(p) == 2 and p[1] not in ('.', ''):
                    try:
                        records[pd.Timestamp(p[0])] = float(p[1])
                    except ValueError:
                        pass
            return pd.Series(records).sort_index() if records else None
        except Exception as e:
            logger.debug(f'[FRED] {sid}: {e}')
            return None

    def _ecb(self, key: str) -> Optional[pd.Series]:
        start = (datetime.now() - timedelta(days=self.history_days)).strftime('%Y-%m-%d')
        try:
            r = requests.get(
                f'https://sdw-wsrest.ecb.europa.eu/service/data/{key}'
                f'?startPeriod={start}&format=csvdata',
                timeout=10
            )
            if r.status_code != 200:
                return None
            lines  = r.text.strip().split('\n')
            hdr    = lines[0].split(',')
            di     = next((i for i,h in enumerate(hdr) if 'TIME_PERIOD' in h), None)
            vi     = next((i for i,h in enumerate(hdr) if 'OBS_VALUE'   in h), None)
            if di is None or vi is None:
                return None
            records = {}
            for line in lines[1:]:
                p = line.split(',')
                if len(p) > max(di, vi):
                    try:
                        records[pd.Timestamp(p[di])] = float(p[vi])
                    except ValueError:
                        pass
            return pd.Series(records).sort_index() if records else None
        except Exception as e:
            logger.debug(f'[ECB] {key}: {e}')
            return None

    def fetch_all(self) -> bool:
        logger.info('[YIELDS] Fetching sovereign yields…')
        cache: Dict[str, Dict[int, pd.Series]] = {}

        cache['US'] = {}
        for mat, sid in self.FRED_US.items():
            s = self._fred(sid)
            if s is not None and len(s) > 20:
                cache['US'][mat] = s
                logger.info(f'  US {mat:2d}Y : {len(s):4d} pts  last={s.iloc[-1]:.3f}%')

        for country in ('DE', 'FR', 'IT'):
            cache[country] = {}
            for mat, ecb_key in self.ECB_SERIES.get(country, {}).items():
                s = self._ecb(ecb_key)
                if s is not None and len(s) > 10:
                    cache[country][mat] = s
                    logger.info(f'  {country} {mat:2d}Y (ECB): {len(s):4d} pts  last={s.iloc[-1]:.3f}%')
                else:
                    fk = self.FRED_INTL.get(country, {}).get(mat)
                    if fk:
                        s = self._fred(fk)
                        if s is not None and len(s) > 5:
                            cache[country][mat] = s
                            logger.info(f'  {country} {mat:2d}Y (FRED): {len(s):4d} pts  last={s.iloc[-1]:.3f}%')
            # FRED fallback pour maturités non couvertes par ECB
            for mat, fk in self.FRED_INTL.get(country, {}).items():
                if mat not in cache[country]:
                    s = self._fred(fk)
                    if s is not None and len(s) > 5:
                        cache[country][mat] = s

        cache['UK'] = {}
        s = self._fred('IRLTLT01GBM156N')
        if s is not None and len(s) > 5:
            cache['UK'][10] = s
            logger.info(f'  UK 10Y (FRED): {len(s):4d} pts  last={s.iloc[-1]:.3f}%')

        cache['JP'] = {}
        s = self._fred('IRLTLT01JPM156N')
        if s is not None and len(s) > 5:
            cache['JP'][10] = s
            logger.info(f'  JP 10Y (FRED): {len(s):4d} pts  last={s.iloc[-1]:.3f}%')

        self._cache = cache
        self.last_update = datetime.now()
        return bool(cache.get('US'))

    def latest(self, country: str, maturity: int) -> Optional[float]:
        s = self._cache.get(country, {}).get(maturity)
        return float(s.iloc[-1]) if s is not None and len(s) > 0 else None

    def history(self, country: str, maturity: int) -> Optional[pd.Series]:
        return self._cache.get(country, {}).get(maturity)

    def history_until(self, country: str, maturity: int, until: pd.Timestamp) -> Optional[pd.Series]:
        """Retourne la série jusqu'à une date donnée (pour backtesting)."""
        s = self.history(country, maturity)
        return s[s.index <= until] if s is not None else None

# ==============================================================================
# YIELD CURVE BUILDER
# ==============================================================================

class YieldCurveBuilder:
    def __init__(self, fetcher: SovereignYieldFetcher):
        self.fetcher  = fetcher
        self._params: Dict[str, Tuple] = {}

    @staticmethod
    def _ns(t, b0, b1, b2, tau):
        if t <= 0: return b0 + b1
        d = (1 - np.exp(-t/tau)) / (t/tau)
        return b0 + b1*d + b2*(d - np.exp(-t/tau))

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
            b0,b1,b2,tau = p
            if tau<=0 or b0<=0: return 1e10
            return np.sum((np.array([self._ns(t,b0,b1,b2,tau) for t in mats]) - obs)**2)

        res = minimize(obj, [obs.mean(), obs[0]-obs[-1], 0.5, 2.0],
                       bounds=[(0.01,20),(-15,15),(-15,15),(0.1,30)], method='L-BFGS-B')
        self._params[country] = tuple(res.x) if res.success else (obs.mean(),-0.3,0.5,2.0)

    def fit_all(self, until: Optional[pd.Timestamp] = None):
        for c in ('US','DE','FR','IT','UK','JP'):
            self.fit(c, until)

    def get_rate(self, country: str, maturity_years: float) -> float:
        if country not in self._params:
            self.fit(country)
        return self._ns(maturity_years, *self._params[country])

# ==============================================================================
# BOND PRICER
# ==============================================================================

class BondPricer:
    def __init__(self, curve: YieldCurveBuilder):
        self.curve = curve

    def price_bond(self, country, coupon_rate, par, years, freq=2) -> Dict:
        pmt = par * coupon_rate / freq
        n   = max(1, int(round(years * freq)))
        pv = dur = conv = 0.0
        for i in range(1, n+1):
            t  = i/freq
            rf = self.curve.get_rate(country, t)/100
            df = 1/(1 + rf/freq)**i
            cf = pmt + (par if i==n else 0)
            pv_cf = cf*df
            pv   += pv_cf
            dur  += t * pv_cf
            conv += t*(t+1/freq)*pv_cf
        if pv == 0:
            return {'price':par,'macaulay_duration':0,'modified_duration':0,'convexity':0,'dv01':0}
        mid_rf  = self.curve.get_rate(country, years/2)/100
        mac_dur = dur/pv
        mod_dur = mac_dur/(1 + mid_rf/freq)
        convex  = conv/(pv*(1+mid_rf/freq)**2)
        return {
            'price':             pv,
            'macaulay_duration': mac_dur,
            'modified_duration': mod_dur,
            'convexity':         convex,
            'dv01':              mod_dur*pv*0.0001,
        }

    def npv_alpha(self, country, coupon_rate, par, years, market_price) -> Dict:
        fair      = self.price_bond(country, coupon_rate, par, years)
        alpha     = market_price - fair['price']
        alpha_pct = alpha/fair['price']*100
        return {'fair_price':fair['price'],'alpha':alpha,'alpha_pct':alpha_pct,
                'alpha_bps':alpha/fair['price']*10_000,**fair}

# ==============================================================================
# SPREAD ANALYZER
# ==============================================================================

class SpreadAnalyzer:
    def __init__(self, fetcher: SovereignYieldFetcher,
                 window=ZSCORE_WINDOW, z_entry=ZSCORE_ENTRY, z_exit=ZSCORE_EXIT):
        self.fetcher  = fetcher
        self.window   = window
        self.z_entry  = z_entry
        self.z_exit   = z_exit

    def spread_series(self, ca, cb, mat,
                      until: Optional[pd.Timestamp]=None) -> Optional[pd.Series]:
        if until is not None:
            sa = self.fetcher.history_until(ca, mat, until)
            sb = self.fetcher.history_until(cb, mat, until)
        else:
            sa = self.fetcher.history(ca, mat)
            sb = self.fetcher.history(cb, mat)
        if sa is None or sb is None: return None
        df = pd.DataFrame({'a':sa,'b':sb}).dropna()
        return (df['a'] - df['b']) if len(df) >= 20 else None

    def zscore(self, series: pd.Series) -> float:
        s = series.dropna()
        if len(s) < 10: return 0.0
        w = min(self.window, len(s))
        mu, sigma = s.iloc[-w:].mean(), s.iloc[-w:].std()
        return float((s.iloc[-1]-mu)/sigma) if sigma > 1e-8 else 0.0

    def analyze_pair(self, ca, cb, mat,
                     until: Optional[pd.Timestamp]=None) -> Dict:
        label = f'{ca}{mat}Y-{cb}{mat}Y'
        ss    = self.spread_series(ca, cb, mat, until)
        if ss is None or len(ss) < 10:
            return {'pair':label,'signal':'NO_DATA','z_score':0.0,
                    'spread_current':None,'spread_mean':None,'spread_std':None,
                    'confidence':0.0,'leg_long':None,'leg_short':None,'n_points':0}

        z       = self.zscore(ss)
        w       = min(self.window, len(ss))
        mu      = float(ss.iloc[-w:].mean())
        sigma   = float(ss.iloc[-w:].std())
        current = float(ss.iloc[-1])

        if   z >  self.z_entry: signal,ll,ls = 'SPREAD_SHORT_A', cb, ca
        elif z < -self.z_entry: signal,ll,ls = 'SPREAD_LONG_A',  ca, cb
        else:                    signal,ll,ls = 'HOLD', None, None

        z_pts  = min(abs(z)/4.0*50, 50.0)
        h_pts  = min(len(ss)/self.window*50, 50.0)

        return {
            'pair':ca+str(mat)+'Y-'+cb+str(mat)+'Y',
            'country_a':ca,'country_b':cb,'maturity':mat,
            'signal':signal,'z_score':z,
            'spread_current':current,'spread_mean':mu,'spread_std':sigma,
            'deviation_bps':(current-mu)*100,
            'confidence':z_pts+h_pts,
            'leg_long':ll,'leg_short':ls,'n_points':len(ss),
        }

    def analyze_all(self, until: Optional[pd.Timestamp]=None) -> List[Dict]:
        results = []
        for ca,cb,mat in SPREAD_PAIRS:
            r = self.analyze_pair(ca,cb,mat,until)
            results.append(r)
            if r['signal'] not in ('HOLD','NO_DATA'):
                logger.info(
                    f'[SPREAD] {r["pair"]:16s} z={r["z_score"]:+5.2f} '
                    f'spread={r["spread_current"]:+6.2f}% '
                    f'(moy={r["spread_mean"]:+5.2f}% ±{r["spread_std"]:.2f}%) '
                    f'dev={r["deviation_bps"]:+5.1f}bps '
                    f'{r["signal"]:20s} conf={r["confidence"]:.0f}%'
                )
        return results

# ==============================================================================
# SIGNAL AGGREGATOR
# ==============================================================================

class SignalAggregator:
    FUTURES_PARAMS: Dict[str,Tuple] = {
        'ZT':  ('US',0.045,100, 2.0), 'ZF': ('US',0.042,100, 5.0),
        'ZN':  ('US',0.043,100,10.0), 'ZB': ('US',0.045,100,30.0),
        'FGBS':('DE',0.010,100, 2.0), 'FGBM':('DE',0.010,100, 5.0),
        'FGBL':('DE',0.020,100,10.0), 'FGBX':('DE',0.025,100,30.0),
        'R':   ('UK',0.035,100,10.0),
    }
    COUNTRY_TO_FUTURES: Dict[Tuple,str] = {
        ('US',2):'ZT',('US',5):'ZF',('US',10):'ZN',('US',30):'ZB',
        ('DE',2):'FGBS',('DE',5):'FGBM',('DE',10):'FGBL',('DE',30):'FGBX',
        ('UK',10):'R',
    }
    FUTURES_EXCHANGE: Dict[str,str] = {
        'ZT':'CBOT','ZF':'CBOT','ZN':'CBOT','ZB':'CBOT',
        'FGBS':'EUREX','FGBM':'EUREX','FGBL':'EUREX','FGBX':'EUREX','R':'LIFFE',
    }

    def __init__(self, pricer: BondPricer):
        self.pricer = pricer

    def npv_check(self, symbol, market_price, leg_role) -> Dict:
        if symbol not in self.FUTURES_PARAMS or market_price is None:
            return {'confirmed':False,'alpha_pct':0.0,'multiplier':0.80,
                    'mod_dur':0,'dv01':0,'fair_price':market_price or 100}
        country,coupon,par,years = self.FUTURES_PARAMS[symbol]
        npv = self.pricer.npv_alpha(country,coupon,par,years,market_price)
        a   = npv['alpha_pct']
        confirmed = (leg_role=='long' and a < -NPV_THRESHOLD) or \
                    (leg_role=='short' and a >  NPV_THRESHOLD)
        opposed   = (leg_role=='long' and a >  NPV_THRESHOLD) or \
                    (leg_role=='short' and a < -NPV_THRESHOLD)
        mult = 1.20 if confirmed else (0.60 if opposed else 0.80)
        return {'confirmed':confirmed,'alpha_pct':a,'alpha_bps':npv['alpha_bps'],
                'fair_price':npv['fair_price'],'mod_dur':npv['modified_duration'],
                'convexity':npv['convexity'],'dv01':npv['dv01'],'multiplier':mult}

    def build_trade(self, sr: Dict, prices: Dict[str,float]) -> Optional[Dict]:
        if sr['signal'] in ('HOLD','NO_DATA'): return None
        ca,cb,mat = sr['country_a'],sr['country_b'],sr['maturity']
        ll = cb if sr['signal']=='SPREAD_SHORT_A' else ca
        ls = ca if sr['signal']=='SPREAD_SHORT_A' else cb
        fl = self.COUNTRY_TO_FUTURES.get((ll,mat))
        fs = self.COUNTRY_TO_FUTURES.get((ls,mat))
        if not fl or not fs: return None

        pl = prices.get(fl)
        ps = prices.get(fs)
        nl = self.npv_check(fl, pl, 'long')
        ns = self.npv_check(fs, ps, 'short')

        # DV01-neutral sizing ratio
        dv01_l = nl['dv01'] if nl['dv01'] > 0 else 1
        dv01_s = ns['dv01'] if ns['dv01'] > 0 else 1
        dv01_ratio = dv01_s / dv01_l   # nb contrats short pour 1 contrat long

        mult = (nl['multiplier']+ns['multiplier'])/2
        conf = min(sr['confidence']*mult, 100.0)

        return {
            'pair':sr['pair'],'signal':sr['signal'],
            'z_score':sr['z_score'],'z_entry':sr['z_score'],
            'spread_current':sr['spread_current'],'spread_mean':sr['spread_mean'],
            'spread_std':sr['spread_std'],'deviation_bps':sr['deviation_bps'],
            'confidence':conf,'dv01_ratio':dv01_ratio,
            'leg_long':{'futures':fl,'exchange':self.FUTURES_EXCHANGE.get(fl,'SMART'),
                        'price':pl,'npv':nl},
            'leg_short':{'futures':fs,'exchange':self.FUTURES_EXCHANGE.get(fs,'SMART'),
                         'price':ps,'npv':ns},
        }

# ==============================================================================
# RISK MANAGER  —  TP / SL DV01-based
# ==============================================================================

class RiskManager:
    """
    Calcule les niveaux TP et SL pour chaque leg d'un trade spread.

    Méthode DV01-based :
    ────────────────────
    La P&L d'un bond futures = DV01 × Δyield (en bps)

    TP : le spread revient vers sa moyenne → Δyield_target = (z_entry - TP_TARGET) × std
    SL : le spread continue de diverger    → Δyield_stop   = SL_EXTEND × std

    On convertit en niveau de prix :
        price_TP = entry_price ± DV01 × bps_TP
        price_SL = entry_price ∓ DV01 × bps_SL
    """

    def compute_tp_sl(self, trade: Dict) -> Dict:
        """
        Retourne les niveaux TP/SL pour les deux legs.
        trade doit contenir : z_entry, spread_std, leg_long, leg_short
        """
        z_entry   = abs(trade.get('z_entry', ZSCORE_ENTRY))
        std       = trade.get('spread_std', 0.20)   # % (e.g. 0.20 = 20 bps)

        # Mouvement de spread espéré pour le TP et le SL (en bps)
        bps_tp = (z_entry - TP_ZSCORE_TARGET) * std * 100
        bps_sl = SL_ZSCORE_EXTEND * std * 100

        result = {}
        for role, leg_key in (('long','leg_long'), ('short','leg_short')):
            leg   = trade.get(leg_key, {})
            entry = leg.get('price')
            dv01  = leg.get('npv', {}).get('dv01', 0)

            if entry is None or dv01 == 0:
                result[leg_key] = {'tp': None, 'sl': None, 'entry': entry}
                continue

            if role == 'long':
                # Long leg : gagne si le yield baisse
                tp = entry + dv01 * bps_tp
                sl = entry - dv01 * bps_sl
            else:
                # Short leg : gagne si le yield monte
                tp = entry - dv01 * bps_tp
                sl = entry + dv01 * bps_sl

            result[leg_key] = {
                'tp':            round(tp, 4),
                'sl':            round(sl, 4),
                'entry':         entry,
                'dv01':          dv01,
                'bps_tp':        bps_tp,
                'bps_sl':        bps_sl,
            }

        return result

    def should_update(self, old_tp: Optional[float], new_tp: Optional[float],
                      current_price: Optional[float]) -> bool:
        """
        Retourne True si le TP/SL doit être remis à jour.
        Critère : le nouveau niveau diffère de plus de TP_SL_UPDATE_THRESHOLD %.
        """
        if old_tp is None or new_tp is None or current_price is None:
            return True
        return abs(new_tp - old_tp) / current_price > TP_SL_UPDATE_THRESHOLD

# ==============================================================================
# PORTFOLIO TRACKER  —  Positions & P&L
# ==============================================================================

PORTFOLIO_FILE = 'portfolio.json'

class PortfolioTracker:
    """
    Suivi des positions ouvertes, historique des trades, P&L.
    Persistance en JSON pour survivre aux redémarrages.
    """

    def __init__(self, filepath: str = PORTFOLIO_FILE):
        self.filepath = filepath
        self.positions: Dict[str, Dict] = {}   # {position_id: position_dict}
        self.closed_trades: List[Dict]   = []
        self.load()

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath) as f:
                    data = json.load(f)
                self.positions    = data.get('positions', {})
                self.closed_trades= data.get('closed_trades', [])
                logger.info(f'[PORTFOLIO] Chargé : {len(self.positions)} positions ouvertes')
            except Exception as e:
                logger.warning(f'[PORTFOLIO] Erreur chargement: {e}')

    def save(self):
        try:
            with open(self.filepath, 'w') as f:
                json.dump({'positions':self.positions,
                           'closed_trades':self.closed_trades}, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f'[PORTFOLIO] Erreur sauvegarde: {e}')

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
                'tp':          tp_sl.get('leg_long',{}).get('tp'),
                'sl':          tp_sl.get('leg_long',{}).get('sl'),
                'tp_order_id': ib_order_ids.get('tp_long'),
                'sl_order_id': ib_order_ids.get('sl_long'),
            },
            'leg_short': {
                'futures':     trade['leg_short']['futures'],
                'entry_price': trade['leg_short']['price'],
                'qty':         qty_short,
                'tp':          tp_sl.get('leg_short',{}).get('tp'),
                'sl':          tp_sl.get('leg_short',{}).get('sl'),
                'tp_order_id': ib_order_ids.get('tp_short'),
                'sl_order_id': ib_order_ids.get('sl_short'),
            },
            'status': 'open',
        }
        self.save()
        return pid

    def close_position(self, pid: str, exit_price_long: float,
                       exit_price_short: float, reason: str = ''):
        if pid not in self.positions:
            return
        pos = self.positions.pop(pid)
        ll  = pos['leg_long']
        ls  = pos['leg_short']
        pnl_long  = (exit_price_long  - ll['entry_price']) * ll['qty'] * 1000
        pnl_short = (ls['entry_price'] - exit_price_short) * ls['qty'] * 1000
        pnl_total = pnl_long + pnl_short
        trade = {**pos, 'exit_time':datetime.now().isoformat(),
                 'exit_price_long':exit_price_long,
                 'exit_price_short':exit_price_short,
                 'pnl_long':pnl_long,'pnl_short':pnl_short,
                 'pnl_total':pnl_total,'close_reason':reason,'status':'closed'}
        self.closed_trades.append(trade)
        self.save()
        return pnl_total

    def update_tp_sl(self, pid: str, leg: str, tp: float, sl: float,
                     tp_order_id=None, sl_order_id=None):
        if pid in self.positions:
            self.positions[pid][leg]['tp'] = tp
            self.positions[pid][leg]['sl'] = sl
            if tp_order_id: self.positions[pid][leg]['tp_order_id'] = tp_order_id
            if sl_order_id: self.positions[pid][leg]['sl_order_id'] = sl_order_id
            self.save()

    # ── Métriques P&L ────────────────────────────────────────────────
    def total_pnl(self) -> float:
        return sum(t.get('pnl_total', 0) for t in self.closed_trades)

    def win_rate(self) -> float:
        if not self.closed_trades: return 0.0
        wins = sum(1 for t in self.closed_trades if t.get('pnl_total',0) > 0)
        return wins / len(self.closed_trades) * 100

    def max_drawdown(self) -> float:
        if not self.closed_trades: return 0.0
        equity = np.cumsum([t.get('pnl_total',0) for t in self.closed_trades])
        peak   = np.maximum.accumulate(equity)
        dd     = equity - peak
        return float(dd.min()) if len(dd) > 0 else 0.0

    def sharpe(self, risk_free: float = 0.04) -> float:
        if len(self.closed_trades) < 2: return 0.0
        pnls = [t.get('pnl_total',0) for t in self.closed_trades]
        mu   = np.mean(pnls)
        std  = np.std(pnls)
        return float(mu/std * np.sqrt(252)) if std > 0 else 0.0

# ==============================================================================
# BACKTESTER  —  Walk-forward sur données FRED historiques
# ==============================================================================

class Backtester:
    """
    Simule la stratégie sur données historiques FRED.

    Walk-forward :
    ─────────────
    • Pour chaque jour de la période :
        1. Calculer z-score sur fenêtre glissante (ZSCORE_WINDOW jours)
        2. Générer les signaux spread
        3. Vérifier TP/SL des positions existantes
        4. Ouvrir les nouvelles positions
        5. Enregistrer le P&L journalier (estimation via DV01 × Δyield)

    P&L estimé (sans prix IBKR) :
        ΔP ≈ DV01_long × Δyield_long + DV01_short × Δyield_short
        où Δyield = variation journalière du taux souverain
    """

    def __init__(self, fetcher: SovereignYieldFetcher, spread_ana: SpreadAnalyzer,
                 aggregator: SignalAggregator, risk_mgr: RiskManager,
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

        # Construire un index journalier commun (jours où US 10Y est disponible)
        us10 = self.fetcher.history('US', 10)
        if us10 is None:
            logger.error('[BACKTEST] Données US 10Y indisponibles')
            return {}

        idx = us10[(us10.index >= dt_from) & (us10.index <= dt_to)].index
        if len(idx) < ZSCORE_WINDOW + 20:
            logger.error(f'[BACKTEST] Historique insuffisant ({len(idx)} jours)')
            return {}

        logger.info(f'[BACKTEST] {date_from} → {dt_to.date()}  ({len(idx)} jours)')

        equity_curve  = [self.capital]
        daily_pnl     = []
        open_positions: List[Dict] = []
        all_trades:     List[Dict] = []
        equity        = self.capital

        for day in idx[ZSCORE_WINDOW:]:
            day_pnl = 0.0

            # ── 1. Analyser les spreads à cette date ─────────────────
            signals = self.spread.analyze_all(until=day)

            # ── 2. Mettre à jour les positions ouvertes ───────────────
            still_open = []
            for pos in open_positions:
                ca,cb,mat = pos['country_a'],pos['country_b'],pos['maturity']
                ss = self.spread.spread_series(ca,cb,mat, until=day)
                if ss is None or len(ss) < 2:
                    still_open.append(pos)
                    continue

                z_now  = self.spread.zscore(ss)
                entry  = pos['entry_price_long']
                dv01_l = pos.get('dv01_long', 1)
                dv01_s = pos.get('dv01_short', 1)

                # Estimation P&L journalier via variation du spread
                spread_delta = float(ss.iloc[-1] - ss.iloc[-2]) * 100  # bps
                if pos['signal'] == 'SPREAD_LONG_A':
                    day_pnl += -dv01_l * spread_delta * pos['qty_long']
                else:
                    day_pnl +=  dv01_l * spread_delta * pos['qty_long']

                # Vérifier TP / SL
                closed = False
                if abs(z_now) < ZSCORE_EXIT:
                    pos['exit_reason'] = 'TP_ZSCORE'
                    pos['pnl'] = day_pnl
                    pos['exit_date'] = str(day.date())
                    all_trades.append(pos)
                    closed = True
                elif pos['signal'] == 'SPREAD_LONG_A'  and z_now < -(ZSCORE_ENTRY + SL_ZSCORE_EXTEND):
                    pos['exit_reason'] = 'SL'
                    pos['exit_date'] = str(day.date())
                    all_trades.append(pos)
                    closed = True
                elif pos['signal'] == 'SPREAD_SHORT_A' and z_now >  (ZSCORE_ENTRY + SL_ZSCORE_EXTEND):
                    pos['exit_reason'] = 'SL'
                    pos['exit_date'] = str(day.date())
                    all_trades.append(pos)
                    closed = True

                if not closed:
                    still_open.append(pos)

            open_positions = still_open

            # ── 3. Ouvrir nouvelles positions ─────────────────────────
            open_pairs = {(p['country_a'],p['country_b'],p['maturity'])
                          for p in open_positions}
            for sr in signals:
                if sr['signal'] in ('HOLD','NO_DATA'): continue
                key = (sr['country_a'],sr['country_b'],sr['maturity'])
                if key in open_pairs: continue  # déjà en position
                if sr['confidence'] < MIN_CONFIDENCE: continue

                # Sizing simplifié pour backtest : 2% du capital par position
                pos_value = equity * 0.02
                # DV01 approximatif selon la maturité
                dv01_approx = {2:2.0, 5:4.5, 10:8.5, 30:18.0}.get(sr['maturity'], 8.5)
                qty = max(1, int(pos_value / (dv01_approx * 10_000)))

                open_positions.append({
                    **sr,
                    'entry_date':    str(day.date()),
                    'entry_z':       sr['z_score'],
                    'qty_long':      qty,
                    'qty_short':     int(qty * 1.0),  # DV01-neutral approx
                    'dv01_long':     dv01_approx * 100,
                    'dv01_short':    dv01_approx * 100,
                    'entry_price_long': 100.0,
                })
                open_pairs.add(key)

            equity     += day_pnl
            daily_pnl.append(day_pnl)
            equity_curve.append(equity)

        # ── Métriques ─────────────────────────────────────────────────
        daily_arr  = np.array(daily_pnl)
        total_ret  = (equity - self.capital) / self.capital * 100
        sharpe     = (daily_arr.mean() / daily_arr.std() * np.sqrt(252)
                      if daily_arr.std() > 0 else 0.0)
        eq         = np.array(equity_curve)
        peak       = np.maximum.accumulate(eq)
        max_dd     = float((eq - peak).min())
        max_dd_pct = max_dd / self.capital * 100
        closed_tr  = [t for t in all_trades]
        wins       = sum(1 for t in closed_tr if t.get('pnl',0) > 0)
        win_rate   = wins/len(closed_tr)*100 if closed_tr else 0.0

        metrics = {
            'date_from':     date_from,
            'date_to':       str(dt_to.date()),
            'n_days':        len(idx) - ZSCORE_WINDOW,
            'initial_cap':   self.capital,
            'final_equity':  equity,
            'total_return':  total_ret,
            'sharpe':        sharpe,
            'max_drawdown':  max_dd,
            'max_dd_pct':    max_dd_pct,
            'n_trades':      len(closed_tr),
            'win_rate':      win_rate,
            'open_positions':len(open_positions),
            'equity_curve':  equity_curve,
            'daily_pnl':     daily_pnl,
            'trades':        closed_tr,
        }
        return metrics

# ==============================================================================
# REPORT GENERATOR  —  Terminal + HTML
# ==============================================================================

class ReportGenerator:
    """
    Génère des rapports formatés :
    • Terminal : tableaux ASCII colorés
    • HTML     : fichier report.html complet avec métriques + positions + trades
    """

    LINE = '─' * 80

    def print_spreads(self, spread_results: List[Dict]):
        print(f'\n{"ANALYSE DES SPREADS":^80}')
        print(self.LINE)
        print(f'{"PAIRE":<20} {"z-score":>8} {"SPREAD":>8} {"MOYENNE":>8} '
              f'{"DÉVIATION":>10} {"SIGNAL":<22} {"CONF":>6}')
        print(self.LINE)
        for r in spread_results:
            if r['spread_current'] is None: continue
            flag = '▲' if r['z_score'] > ZSCORE_ENTRY else ('▼' if r['z_score'] < -ZSCORE_ENTRY else ' ')
            print(f'{r["pair"]:<20} {r["z_score"]:>+8.2f} '
                  f'{r["spread_current"]:>+8.3f}% {r["spread_mean"]:>+8.3f}% '
                  f'{r["deviation_bps"]:>+9.1f}bps  '
                  f'{flag} {r["signal"]:<20} {r["confidence"]:>5.0f}%')
        print(self.LINE)

    def print_positions(self, portfolio: PortfolioTracker):
        print(f'\n{"POSITIONS OUVERTES":^80}')
        print(self.LINE)
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
        print(self.LINE)

    def print_summary(self, portfolio: PortfolioTracker):
        print(f'\n{"RÉSUMÉ PERFORMANCE":^80}')
        print(self.LINE)
        print(f'  Trades clôturés : {len(portfolio.closed_trades)}')
        print(f'  P&L total       : ${portfolio.total_pnl():>+12,.0f}')
        print(f'  Win rate        : {portfolio.win_rate():>6.1f}%')
        print(f'  Max drawdown    : ${portfolio.max_drawdown():>+12,.0f}')
        print(f'  Sharpe (annuel) : {portfolio.sharpe():>6.2f}')
        print(self.LINE)

    def print_backtest(self, metrics: Dict):
        if not metrics: return
        print(f'\n{"RÉSULTATS BACKTEST":^80}')
        print(self.LINE)
        print(f'  Période         : {metrics["date_from"]} → {metrics["date_to"]}')
        print(f'  Capital initial : ${metrics["initial_cap"]:>12,.0f}')
        print(f'  Capital final   : ${metrics["final_equity"]:>12,.0f}')
        print(f'  Rendement total : {metrics["total_return"]:>+7.2f}%')
        print(f'  Sharpe ratio    : {metrics["sharpe"]:>7.2f}')
        print(f'  Max Drawdown    : ${metrics["max_drawdown"]:>+12,.0f}  '
              f'({metrics["max_dd_pct"]:+.2f}%)')
        print(f'  Trades          : {metrics["n_trades"]}  '
              f'(win rate {metrics["win_rate"]:.1f}%)')
        print(self.LINE)
        if metrics.get('trades'):
            print(f'\n  {"TRADE":<22} {"ENTRÉE":<12} {"SORTIE":<12} '
                  f'{"RAISON":<15} {"P&L":>10}')
            print('  ' + '─'*72)
            for t in metrics['trades'][-20:]:  # 20 derniers
                print(f'  {t["pair"]:<22} {t["entry_date"]:<12} '
                      f'{t.get("exit_date","ouvert"):<12} '
                      f'{t.get("exit_reason",""):<15} '
                      f'${t.get("pnl",0):>+9,.0f}')

    def save_html(self, portfolio: PortfolioTracker,
                  spread_results: List[Dict],
                  backtest: Optional[Dict] = None,
                  filepath: str = 'report.html'):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Sections HTML
        spreads_rows = ''
        for r in spread_results:
            if r['spread_current'] is None: continue
            color = '#e74c3c' if abs(r['z_score']) > ZSCORE_ENTRY else '#2ecc71'
            spreads_rows += (
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

        perf_section = ''
        if portfolio.closed_trades:
            perf_section = f'''
            <h2>Performance</h2>
            <table><tr><th>Métrique</th><th>Valeur</th></tr>
            <tr><td>P&L Total</td><td>${portfolio.total_pnl():+,.0f}</td></tr>
            <tr><td>Trades</td><td>{len(portfolio.closed_trades)}</td></tr>
            <tr><td>Win Rate</td><td>{portfolio.win_rate():.1f}%</td></tr>
            <tr><td>Max Drawdown</td><td>${portfolio.max_drawdown():+,.0f}</td></tr>
            <tr><td>Sharpe</td><td>{portfolio.sharpe():.2f}</td></tr>
            </table>'''

        bt_section = ''
        if backtest:
            bt_section = f'''
            <h2>Backtest {backtest["date_from"]} → {backtest["date_to"]}</h2>
            <table><tr><th>Métrique</th><th>Valeur</th></tr>
            <tr><td>Rendement</td><td>{backtest["total_return"]:+.2f}%</td></tr>
            <tr><td>Sharpe</td><td>{backtest["sharpe"]:.2f}</td></tr>
            <tr><td>Max Drawdown</td><td>{backtest["max_dd_pct"]:+.2f}%</td></tr>
            <tr><td>Win Rate</td><td>{backtest["win_rate"]:.1f}%</td></tr>
            <tr><td>Trades</td><td>{backtest["n_trades"]}</td></tr>
            </table>'''

        html = f'''<!DOCTYPE html><html><head><meta charset="utf-8">
        <title>Bond Arb Report — {now}</title>
        <style>
          body{{font-family:monospace;background:#1a1a2e;color:#eee;padding:2rem}}
          h1{{color:#00d4ff}} h2{{color:#0ea5e9;margin-top:2rem}}
          table{{border-collapse:collapse;width:100%;margin-top:1rem}}
          th{{background:#0f3460;padding:8px;text-align:left;color:#00d4ff}}
          td{{padding:6px 8px;border-bottom:1px solid #333}}
          tr:hover{{background:#0f3460}}
        </style></head><body>
        <h1>Government Bond Arbitrage — Report</h1>
        <p>Généré le {now}</p>
        <h2>Spreads Inter-pays</h2>
        <table><tr><th>Paire</th><th>z-score</th><th>Spread</th><th>Moyenne</th>
        <th>Déviation (bps)</th><th>Signal</th><th>Conf</th></tr>
        {spreads_rows}</table>
        <h2>Positions Ouvertes ({len(portfolio.positions)})</h2>
        <table><tr><th>Paire</th><th>Entrée</th><th>z entrée</th>
        <th>Legs</th><th>TP</th><th>SL</th></tr>
        {pos_rows}</table>
        {perf_section}
        {bt_section}
        </body></html>'''

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info(f'[REPORT] Sauvegardé → {filepath}')
        return filepath

# ==============================================================================
# DYNAMIC POSITION SIZER
# ==============================================================================

class DynamicPositionSizer:
    def __init__(self, min_pct=MIN_POSITION_PCT, max_pct=MAX_POSITION_PCT,
                 min_conf=MIN_CONFIDENCE):
        self.min_pct = min_pct; self.max_pct = max_pct; self.min_conf = min_conf

    def calculate(self, equity, confidence, price, dv01_ratio=1.0) -> Dict:
        if confidence < self.min_conf or equity <= 0 or price <= 0:
            return {'can_trade':False,'reason':f'conf={confidence:.0f}%',
                    'qty_long':0,'qty_short':0}
        norm   = max(0.0, min(1.0,(confidence-self.min_conf)/(100.0-self.min_conf)))
        pct    = self.min_pct + (self.max_pct-self.min_pct)*norm
        value  = equity * pct / 100.0
        q_long = max(1, int(value/(price*1_000)))
        q_short= max(1, int(q_long * dv01_ratio))
        return {'can_trade':True,'qty_long':q_long,'qty_short':q_short,
                'pct':pct,'value':q_long*price*1_000}

# ==============================================================================
# GOVERNMENT BONDS BOT  —  Live Trading
# ==============================================================================

class GovernmentBondsBot:
    """
    Bot principal — cycle complet à chaque scan :
    1. Rebalancing quotidien (08h00 ET) : nouveaux signaux + cancel/reset TP/SL
    2. Monitoring continu (toutes les 5 min) : vérification TP/SL uniquement
    3. Les TP/SL sont placés en ordres OCA (One Cancels All) dans IB
    """

    def __init__(self, ib_host=IB_HOST, ib_port=IB_PORT):
        from ib_insync import IB, Future, LimitOrder, StopOrder, Order
        self.IB, self.Future = IB, Future
        self.LimitOrder, self.StopOrder = LimitOrder, StopOrder

        self.ib   = IB()
        self.host = ib_host; self.port = ib_port

        self.fetcher    = SovereignYieldFetcher(history_days=400)
        self.curve      = YieldCurveBuilder(self.fetcher)
        self.pricer     = BondPricer(self.curve)
        self.spread_ana = SpreadAnalyzer(self.fetcher)
        self.aggregator = SignalAggregator(self.pricer)
        self.risk_mgr   = RiskManager()
        self.portfolio  = PortfolioTracker()
        self.sizer      = DynamicPositionSizer()
        self.reporter   = ReportGenerator()

        self.equity          = 0.0
        self.running         = False
        self.last_rebalance  = None
        self.last_data_refresh = None

    # ── Connexion ────────────────────────────────────────────────────
    async def connect(self) -> bool:
        try:
            await self.ib.connectAsync(self.host, self.port, clientId=3)
            logger.info(f'[CONNECTED] {self.host}:{self.port}')
            await self._refresh_equity()
            return True
        except Exception as e:
            logger.error(f'[CONNECTION FAILED] {e}'); return False

    async def _refresh_equity(self):
        try:
            for av in self.ib.accountValues():
                if av.tag == 'NetLiquidation' and av.currency == 'USD':
                    self.equity = float(av.value); return
        except Exception: pass
        self.equity = 1_000_000.0

    # ── Contrats / Prix ──────────────────────────────────────────────
    async def _front_month(self, symbol, exchange):
        try:
            raw     = self.Future(symbol=symbol, exchange=exchange, currency='USD')
            details = await self.ib.reqContractDetailsAsync(raw)
            if not details: return None
            details.sort(key=lambda d: d.contract.lastTradeDateOrContractMonth)
            front  = details[0].contract
            expiry = datetime.strptime(front.lastTradeDateOrContractMonth[:8],'%Y%m%d')
            if (expiry-datetime.now()).days < 7 and len(details)>1:
                front  = details[1].contract
            return front
        except Exception as e:
            logger.debug(f'[CONTRACT] {symbol}: {e}'); return None

    async def _live_price(self, contract) -> Optional[float]:
        try:
            self.ib.reqMktData(contract,'',False,False)
            await asyncio.sleep(2)
            t = self.ib.ticker(contract)
            for v in [t.last, t.close,
                      (t.bid+t.ask)/2 if t.bid and t.ask and t.bid>0 else None]:
                if v and not np.isnan(v) and v>0:
                    self.ib.cancelMktData(contract); return float(v)
            self.ib.cancelMktData(contract)
        except Exception: pass
        return None

    async def _fetch_all_prices(self) -> Dict[str,Optional[float]]:
        prices = {}
        for (_,__), sym in self.aggregator.COUNTRY_TO_FUTURES.items():
            if sym in prices: continue
            exch    = self.aggregator.FUTURES_EXCHANGE.get(sym,'SMART')
            contract= await self._front_month(sym, exch)
            if contract:
                prices[sym] = await self._live_price(contract)
                logger.info(f'  {sym:6s}: {prices[sym]}')
            else:
                prices[sym] = None
            await asyncio.sleep(0.3)
        return prices

    # ── Ordres TP / SL ───────────────────────────────────────────────
    async def _cancel_order(self, order_id):
        if order_id is None: return
        try:
            open_orders = self.ib.openOrders()
            for o in open_orders:
                if o.orderId == order_id:
                    self.ib.cancelOrder(o); return
        except Exception as e:
            logger.debug(f'[CANCEL] {order_id}: {e}')

    async def _place_tp_sl(self, contract, action_entry, qty, tp_price, sl_price) -> Dict:
        """
        Place une paire d'ordres TP (limit) + SL (stop) en OCA.
        Retourne les order IDs.
        """
        from ib_insync import LimitOrder, StopOrder
        close_action = 'SELL' if action_entry == 'BUY' else 'BUY'
        oca_group    = f'OCA_{contract.localSymbol}_{datetime.now().strftime("%H%M%S")}'

        if DRY_RUN:
            logger.info(f'[DRY RUN] TP {close_action} {qty}x @ {tp_price:.3f}  '
                        f'SL {close_action} {qty}x @ {sl_price:.3f}')
            return {'tp_order_id': None, 'sl_order_id': None}

        tp_order = LimitOrder(close_action, qty, tp_price)
        sl_order = StopOrder(close_action, qty, sl_price)
        tp_order.ocaGroup = oca_group; tp_order.ocaType = 1
        sl_order.ocaGroup = oca_group; sl_order.ocaType = 1

        tp_trade = self.ib.placeOrder(contract, tp_order)
        sl_trade = self.ib.placeOrder(contract, sl_order)
        return {'tp_order_id': tp_trade.order.orderId,
                'sl_order_id': sl_trade.order.orderId}

    # ── Rebalancing ──────────────────────────────────────────────────
    async def rebalance(self):
        """
        Cycle complet de rebalancing :
        1. Analyser tous les spreads
        2. Fermer positions dont le z-score est revenu (≤ ZSCORE_EXIT)
        3. Cancel + recalculer TP/SL sur positions encore ouvertes
        4. Ouvrir nouvelles positions
        """
        logger.info('\n' + '='*80)
        logger.info(f'[REBALANCE] {datetime.now():%Y-%m-%d %H:%M:%S}')
        logger.info('='*80)

        await self._refresh_equity()
        spread_results = self.spread_ana.analyze_all()
        prices         = await self._fetch_all_prices()

        sr_map = {(r['country_a'],r['country_b'],r['maturity']): r
                  for r in spread_results}

        # ── 1. Fermer positions dont le spread a revert ───────────────
        for pid, pos in list(self.portfolio.positions.items()):
            ca = pos['leg_long']['futures'][:2]  # approximation
            key_matches = [k for k in sr_map if any(s in pid for s in [k[0],k[1]])]
            # Chercher le z-score actuel pour cette position
            for sr in spread_results:
                if sr['pair'] == pos['pair']:
                    if abs(sr['z_score']) < ZSCORE_EXIT:
                        logger.info(f'[CLOSE] {pos["pair"]} z={sr["z_score"]:+.2f} → TP')
                        # Annuler ordres TP/SL existants
                        for leg_key in ('leg_long','leg_short'):
                            await self._cancel_order(pos[leg_key].get('tp_order_id'))
                            await self._cancel_order(pos[leg_key].get('sl_order_id'))
                        ep_l = prices.get(pos['leg_long']['futures'])  or pos['leg_long']['entry_price']
                        ep_s = prices.get(pos['leg_short']['futures']) or pos['leg_short']['entry_price']
                        pnl  = self.portfolio.close_position(pid, ep_l, ep_s, 'TP_ZSCORE')
                        logger.info(f'[CLOSED] {pos["pair"]} P&L estimé: ${pnl:+,.0f}')

        # ── 2. Mettre à jour TP/SL positions existantes ───────────────
        for pid, pos in self.portfolio.positions.items():
            trade_proxy = {
                'pair':       pos['pair'],
                'signal':     pos['signal'],
                'z_entry':    pos.get('z_entry', ZSCORE_ENTRY),
                'spread_std': next((r['spread_std'] for r in spread_results
                                    if r['pair']==pos['pair']), 0.20),
                'leg_long':  {'price': prices.get(pos['leg_long']['futures']),
                              'npv':   {'dv01': pos['leg_long'].get('dv01',8.5)}},
                'leg_short': {'price': prices.get(pos['leg_short']['futures']),
                              'npv':   {'dv01': pos['leg_short'].get('dv01',8.5)}},
            }
            new_tp_sl = self.risk_mgr.compute_tp_sl(trade_proxy)

            for leg_key, action_entry in (('leg_long','BUY'),('leg_short','SELL')):
                leg    = pos[leg_key]
                new_tp = new_tp_sl.get(leg_key,{}).get('tp')
                new_sl = new_tp_sl.get(leg_key,{}).get('sl')
                cur_p  = prices.get(leg['futures'])

                if self.risk_mgr.should_update(leg.get('tp'), new_tp, cur_p):
                    logger.info(f'[TP/SL UPDATE] {pid} {leg_key}: '
                                f'TP {leg.get("tp")} → {new_tp}  '
                                f'SL {leg.get("sl")} → {new_sl}')
                    await self._cancel_order(leg.get('tp_order_id'))
                    await self._cancel_order(leg.get('sl_order_id'))
                    if cur_p and new_tp and new_sl:
                        contract = await self._front_month(
                            leg['futures'],
                            self.aggregator.FUTURES_EXCHANGE.get(leg['futures'],'SMART'))
                        if contract:
                            ids = await self._place_tp_sl(
                                contract, action_entry, leg['qty'], new_tp, new_sl)
                            self.portfolio.update_tp_sl(
                                pid, leg_key, new_tp, new_sl,
                                ids['tp_order_id'], ids['sl_order_id'])

        # ── 3. Nouvelles positions ─────────────────────────────────────
        open_pairs = {pos['pair'] for pos in self.portfolio.positions.values()}

        for sr in spread_results:
            trade = self.aggregator.build_trade(sr, prices)
            if trade is None: continue
            if trade['pair'] in open_pairs: continue
            if trade['confidence'] < MIN_CONFIDENCE: continue

            ref_price = trade['leg_long']['price'] or 100.0
            sizing    = self.sizer.calculate(
                self.equity, trade['confidence'], ref_price, trade['dv01_ratio'])
            if not sizing['can_trade']: continue

            logger.info(
                f'\n[NEW TRADE] {trade["pair"]} | {trade["signal"]}\n'
                f'  z={trade["z_score"]:+.2f} | dev={trade["deviation_bps"]:+.1f}bps '
                f'| conf={trade["confidence"]:.0f}%\n'
                f'  LONG  {trade["leg_long"]["futures"]} x{sizing["qty_long"]} '
                f'@ {trade["leg_long"]["price"]}  NPV {trade["leg_long"]["npv"]["alpha_pct"]:+.3f}%\n'
                f'  SHORT {trade["leg_short"]["futures"]} x{sizing["qty_short"]} '
                f'@ {trade["leg_short"]["price"]}  NPV {trade["leg_short"]["npv"]["alpha_pct"]:+.3f}%'
            )

            tp_sl    = self.risk_mgr.compute_tp_sl(trade)
            order_ids= {}

            for leg_key, fut_key, action in (
                ('leg_long', 'leg_long',  'BUY'),
                ('leg_short','leg_short','SELL')
            ):
                leg      = trade[leg_key]
                qty      = sizing['qty_long'] if leg_key=='leg_long' else sizing['qty_short']
                contract = await self._front_month(
                    leg['futures'], leg['exchange'])
                if contract and not DRY_RUN:
                    limit = leg['price'] * (1.001 if action=='BUY' else 0.999)
                    from ib_insync import LimitOrder
                    self.ib.placeOrder(contract, LimitOrder(action, qty, limit))

                if DRY_RUN:
                    logger.info(f'[DRY RUN] {action} {qty}x {leg["futures"]} '
                                f'@ {leg["price"]}')

                # TP/SL pour ce leg
                tp = tp_sl.get(leg_key,{}).get('tp')
                sl = tp_sl.get(leg_key,{}).get('sl')
                if contract and tp and sl:
                    ids = await self._place_tp_sl(contract, action, qty, tp, sl)
                    order_ids[f'tp_{leg_key.replace("leg_","")}'] = ids['tp_order_id']
                    order_ids[f'sl_{leg_key.replace("leg_","")}'] = ids['sl_order_id']

            self.portfolio.open_position(
                {**trade,
                 'leg_long': {**trade['leg_long'], 'dv01': trade['leg_long']['npv']['dv01']},
                 'leg_short':{**trade['leg_short'],'dv01': trade['leg_short']['npv']['dv01']}},
                sizing['qty_long'], sizing['qty_short'], tp_sl, order_ids)

        self.last_rebalance = datetime.now()
        self.reporter.print_spreads(spread_results)
        self.reporter.print_positions(self.portfolio)
        self.reporter.print_summary(self.portfolio)

    # ── Boucle principale ────────────────────────────────────────────
    async def run(self):
        self.running = True
        logger.info('\n' + '='*80)
        logger.info('[START] Government Bond Arbitrage v2.0')
        logger.info(f'  Z-score entry/exit/stop : ±{ZSCORE_ENTRY} / ±{ZSCORE_EXIT} / ±{ZSCORE_STOP}')
        logger.info(f'  NPV threshold           : {NPV_THRESHOLD}%')
        logger.info(f'  Rebalancing             : 1× par jour à {REBALANCE_HOUR}h UTC')
        logger.info(f'  Monitoring TP/SL        : toutes les {MONITOR_INTERVAL}s')
        logger.info(f'  Dry run                 : {DRY_RUN}')
        logger.info('='*80)

        self.fetcher.fetch_all()
        self.curve.fit_all()
        self.last_data_refresh = datetime.now()

        # Premier rebalancing immédiat au démarrage
        await self.rebalance()

        try:
            while self.running:
                now = datetime.now()

                # Refresh données toutes les DATA_REFRESH_H heures
                if self.last_data_refresh and \
                   (now - self.last_data_refresh).seconds > DATA_REFRESH_H * 3600:
                    logger.info('[DATA REFRESH] Mise à jour taux souverains…')
                    self.fetcher.fetch_all()
                    self.curve.fit_all()
                    self.last_data_refresh = now

                # Rebalancing quotidien à REBALANCE_HOUR heure UTC
                should_rebal = (
                    self.last_rebalance is None or
                    (now.hour == REBALANCE_HOUR and now.date() > self.last_rebalance.date())
                )
                if should_rebal:
                    await self.rebalance()
                else:
                    logger.info(f'[MONITOR] {now:%H:%M:%S} — '
                                f'{len(self.portfolio.positions)} positions ouvertes')

                await asyncio.sleep(MONITOR_INTERVAL)

        except asyncio.CancelledError:
            logger.info('[STOPPED]')
        except Exception as e:
            logger.error(f'[CRASH] {e}')
            import traceback; traceback.print_exc()
        finally:
            self.running = False
            try: self.ib.disconnect()
            except Exception: pass
            logger.info('[DISCONNECTED]')

# ==============================================================================
# CLI  —  Répertoire de commandes
# ==============================================================================

HELP_TEXT = """
╔══════════════════════════════════════════════════════════════════════════════╗
║          GOVERNMENT BOND ARBITRAGE STRATEGY  —  v2.0                       ║
║          Spread Inter-pays  +  NPV/DCF Confirmation                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  COMMANDES DISPONIBLES                                                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ▶ START — Lancer le bot en trading live                                    ║
║    python bonds_arbitrage_strategy.py start                                 ║
║    python bonds_arbitrage_strategy.py start --paper          (paper trading)║
║    python bonds_arbitrage_strategy.py start --port 7496      (live trading) ║
║    python bonds_arbitrage_strategy.py start --dry-run false  (ordres réels) ║
║                                                                              ║
║  ▶ BACKTEST — Simuler la stratégie sur données historiques FRED             ║
║    python bonds_arbitrage_strategy.py backtest                              ║
║    python bonds_arbitrage_strategy.py backtest --from 2020-01-01           ║
║    python bonds_arbitrage_strategy.py backtest --from 2019-01-01 --to 2024-01-01 ║
║    python bonds_arbitrage_strategy.py backtest --capital 500000             ║
║    python bonds_arbitrage_strategy.py backtest --html         (+ rapport HTML)║
║                                                                              ║
║  ▶ REPORT — Générer un rapport de performance                               ║
║    python bonds_arbitrage_strategy.py report                                ║
║    python bonds_arbitrage_strategy.py report --html           (fichier HTML)║
║                                                                              ║
║  ▶ POSITIONS — Afficher les positions ouvertes                              ║
║    python bonds_arbitrage_strategy.py positions                             ║
║                                                                              ║
║  ▶ SPREADS — Analyser les spreads actuels (sans IBKR)                      ║
║    python bonds_arbitrage_strategy.py spreads                               ║
║                                                                              ║
║  ▶ HELP — Afficher cette page                                               ║
║    python bonds_arbitrage_strategy.py help                                  ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PÉRIODICITÉ RECOMMANDÉE                                                    ║
║  • Bot en continu (start) — gère lui-même le timing                        ║
║  • Rebalancing signaux : 1× par jour à 14h UTC (10h ET / 15h Paris)        ║
║  • Monitoring TP/SL    : toutes les 5 minutes (automatique)                ║
║  • Refresh données     : toutes les 6 heures  (automatique)                ║
║  • Backtest            : à la demande avant chaque changement de paramètre  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PAIRES SURVEILLÉES                                                          ║
║  US10Y-DE10Y  US2Y-DE2Y   US5Y-DE5Y   US10Y-UK10Y                         ║
║  US10Y-JP10Y  US10Y-FR10Y  DE10Y-IT10Y  DE10Y-FR10Y                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  PARAMÈTRES CLÉS (modifiables en haut du fichier)                           ║
║  ZSCORE_ENTRY=2.0  ZSCORE_EXIT=0.5  ZSCORE_STOP=3.5                       ║
║  MIN_CONFIDENCE=30%  MIN/MAX_POSITION=1-5%                                 ║
║  TP_ZSCORE_TARGET=0.3  SL_ZSCORE_EXTEND=1.0                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""


def _build_spread_context():
    """Construit le contexte d'analyse spread sans IBKR."""
    fetcher    = SovereignYieldFetcher(history_days=400)
    fetcher.fetch_all()
    curve      = YieldCurveBuilder(fetcher)
    curve.fit_all()
    pricer     = BondPricer(curve)
    spread_ana = SpreadAnalyzer(fetcher)
    return fetcher, curve, pricer, spread_ana


def cmd_spreads():
    print('\nFetch des taux souverains en cours…')
    fetcher, curve, pricer, spread_ana = _build_spread_context()
    results = spread_ana.analyze_all()
    ReportGenerator().print_spreads(results)


def cmd_positions():
    portfolio = PortfolioTracker()
    reporter  = ReportGenerator()
    reporter.print_positions(portfolio)
    reporter.print_summary(portfolio)


def cmd_report(html: bool = False):
    fetcher, curve, pricer, spread_ana = _build_spread_context()
    results   = spread_ana.analyze_all()
    portfolio = PortfolioTracker()
    reporter  = ReportGenerator()
    reporter.print_spreads(results)
    reporter.print_positions(portfolio)
    reporter.print_summary(portfolio)
    if html:
        path = reporter.save_html(portfolio, results)
        print(f'\n→ Rapport HTML : {path}')


def cmd_backtest(date_from: str, date_to: Optional[str],
                 capital: float, html: bool):
    dt_label = date_to or "aujourd'hui"
    print(f'\nBacktest {date_from} -> {dt_label}  capital={capital:,.0f}$')
    fetcher, curve, pricer, spread_ana = _build_spread_context()
    aggregator = SignalAggregator(pricer)
    risk_mgr   = RiskManager()
    bt         = Backtester(fetcher, spread_ana, aggregator, risk_mgr, capital)
    metrics    = bt.run(date_from, date_to)
    if not metrics:
        print('Backtest impossible (données insuffisantes).')
        return
    reporter = ReportGenerator()
    reporter.print_backtest(metrics)
    if html:
        portfolio = PortfolioTracker()
        path = reporter.save_html(portfolio, [], backtest=metrics,
                                   filepath='backtest_report.html')
        print(f'\n→ Rapport HTML : {path}')


def cmd_start(port: int, dry_run: bool):
    from ib_insync import IB
    global DRY_RUN
    DRY_RUN = dry_run
    bot = GovernmentBondsBot(ib_host=IB_HOST, ib_port=port)
    async def _run():
        if not await bot.connect():
            print('[ERREUR] Connexion IBKR impossible.')
            print('  1. TWS ouvert ?')
            print('  2. API activée ? → File → Global Config → API → Enable')
            print(f'  3. Port : {port} (7497=Paper, 7496=Live)')
            return
        try:
            await bot.run()
        except KeyboardInterrupt:
            bot.running = False
    asyncio.run(_run())


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        prog='bonds_arbitrage_strategy.py',
        description='Government Bond Arbitrage — Spread Inter-pays + NPV',
        add_help=False
    )
    sub = parser.add_subparsers(dest='command')

    # start
    p_start = sub.add_parser('start')
    p_start.add_argument('--paper',    action='store_true', default=True)
    p_start.add_argument('--port',     type=int, default=IB_PORT)
    p_start.add_argument('--dry-run',  type=lambda x: x.lower()!='false',
                         default=True, dest='dry_run')

    # backtest
    p_bt = sub.add_parser('backtest')
    p_bt.add_argument('--from', dest='date_from', default=BT_DEFAULT_FROM)
    p_bt.add_argument('--to',   dest='date_to',   default=None)
    p_bt.add_argument('--capital', type=float,    default=BT_DEFAULT_CAPITAL)
    p_bt.add_argument('--html', action='store_true')

    # report
    p_rep = sub.add_parser('report')
    p_rep.add_argument('--html', action='store_true')

    # positions / spreads / help
    sub.add_parser('positions')
    sub.add_parser('spreads')
    sub.add_parser('help')

    args = parser.parse_args()

    if args.command == 'start':
        cmd_start(port=args.port, dry_run=args.dry_run)
    elif args.command == 'backtest':
        cmd_backtest(args.date_from, args.date_to, args.capital, args.html)
    elif args.command == 'report':
        cmd_report(args.html)
    elif args.command == 'positions':
        cmd_positions()
    elif args.command == 'spreads':
        cmd_spreads()
    else:
        print(HELP_TEXT)


if __name__ == '__main__':
    main()
