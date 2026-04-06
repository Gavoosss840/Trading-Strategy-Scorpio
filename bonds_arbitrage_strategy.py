"""
================================================================================
Government Bond Arbitrage Strategy
================================================================================

Combinaison de deux sources d'alpha complémentaires :

  SIGNAL 1 — Spread Inter-pays  (signal directionnel macro)
      - Fetch des taux souverains US / DE / UK / JP / FR / IT via FRED + ECB
      - Calcul des spreads historiques par paire et maturité
      - Z-score de déviation par rapport à la moyenne historique
      - Signal quand |z| > 2 : mean-reversion attendu

  SIGNAL 2 — NPV / DCF  (timing et optimisation d'entrée)
      - Courbe Nelson-Siegel par pays
      - Pricing DCF des futures (ZN, FGBL, Gilt…)
      - Si NPV confirme le spread signal → confidence boostée
      - Duration / Convexity / DV01 pour le sizing

  EXECUTION — Interactive Brokers
      - Futures liquides : ZT/ZF/ZN/ZB (CBOT), FGBS/FGBM/FGBL/FGBX (Eurex)
      - Position sizing dynamique 1-5% selon confidence
      - Rollover automatique front-month

================================================================================
"""

# ==============================================================================
# CONFIGURATION
# ==============================================================================

IB_HOST          = '127.0.0.1'
IB_PORT          = 7497          # 7497 = Paper, 7496 = Live

ZSCORE_WINDOW    = 252           # jours pour calcul z-score (≈ 1 an)
ZSCORE_ENTRY     = 2.0           # seuil d'entrée   |z| > 2.0
ZSCORE_EXIT      = 0.5           # seuil de sortie  |z| < 0.5
NPV_THRESHOLD    = 0.05          # alpha NPV minimum en % pour confirmation
SCAN_INTERVAL    = 300           # secondes entre chaque scan (5 min)
MIN_CONFIDENCE   = 30.0
MIN_POSITION_PCT = 1.0
MAX_POSITION_PCT = 5.0
DRY_RUN          = True          # False = ordres réels

# Paires à surveiller : (pays_A, pays_B, maturité_années)
SPREAD_PAIRS = [
    ('US', 'DE', 10),   # US 10Y vs Bund 10Y  — la paire de référence mondiale
    ('US', 'DE',  2),   # US 2Y  vs Schatz 2Y
    ('US', 'DE',  5),   # US 5Y  vs Bobl 5Y
    ('US', 'UK', 10),   # US 10Y vs Gilt 10Y
    ('US', 'JP', 10),   # US 10Y vs JGB 10Y
    ('US', 'FR', 10),   # US 10Y vs OAT 10Y
    ('DE', 'IT', 10),   # Bund vs BTP  — spread peripherique EU
    ('DE', 'FR', 10),   # Bund vs OAT
]

# ==============================================================================
# IMPORTS
# ==============================================================================

import numpy as np
import pandas as pd
import requests
from scipy.optimize import minimize
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from ib_insync import IB, Future, LimitOrder
import asyncio
import logging
import sys

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
    """
    Fetch sovereign bond yields pour US, DE, UK, JP, FR, IT.

    Sources :
    ─────────
    • US       → FRED (daily, fiable, gratuit)
    • EU pays  → ECB Statistical Data Warehouse API (daily)
                 Fallback → FRED IRLTLT01XXM156N (mensuel)
    • UK       → BoE API (daily)
                 Fallback → FRED IRLTLT01GBM156N
    • JP       → FRED IRLTLT01JPM156N (mensuel)
    """

    # ── FRED series IDs ────────────────────────────────────────────────
    FRED_US: Dict[int, str] = {
        1:  'DGS1',
        2:  'DGS2',
        5:  'DGS5',
        10: 'DGS10',
        30: 'DGS30',
    }

    FRED_INTL: Dict[str, Dict[int, str]] = {
        # Mensuel OECD/FRED — fallback fiable
        'DE': {2: 'IRLTST01DEM156N', 10: 'IRLTLT01DEM156N'},
        'FR': {10: 'IRLTLT01FRM156N'},
        'IT': {10: 'IRLTLT01ITM156N'},
        'UK': {10: 'IRLTLT01GBM156N'},
        'JP': {10: 'IRLTLT01JPM156N'},
    }

    # ── ECB SDW series IDs (daily) ─────────────────────────────────────
    ECB_SERIES: Dict[str, Dict[int, str]] = {
        'DE': {
            2:  'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y',
            5:  'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y',
            10: 'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y',
        },
        'FR': {10: 'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y'},  # zone euro proxy
        'IT': {10: 'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y'},
    }

    def __init__(self, history_days: int = 400):
        """
        history_days : combien de jours d'historique fetcher
                       (min 300 pour avoir un z-score sur 252j)
        """
        self.history_days = history_days
        # Cache : {country: {maturity: pd.Series(date→yield%)}}
        self._cache: Dict[str, Dict[int, pd.Series]] = {}
        self.last_update: Optional[datetime] = None

    # ------------------------------------------------------------------
    def _fred_series(self, series_id: str) -> Optional[pd.Series]:
        """Fetch une série FRED, retourne pd.Series indexed par date."""
        start = (datetime.now() - timedelta(days=self.history_days)).strftime('%Y-%m-%d')
        url   = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start}'
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                return None
            lines = r.text.strip().split('\n')
            records = {}
            for line in lines[1:]:
                parts = line.split(',')
                if len(parts) == 2 and parts[1] not in ('.', ''):
                    try:
                        records[pd.Timestamp(parts[0])] = float(parts[1])
                    except ValueError:
                        pass
            if records:
                return pd.Series(records).sort_index()
        except Exception as e:
            logger.debug(f'[FRED] {series_id}: {e}')
        return None

    def _ecb_series(self, series_key: str) -> Optional[pd.Series]:
        """Fetch une série ECB SDW (daily)."""
        start = (datetime.now() - timedelta(days=self.history_days)).strftime('%Y-%m-%d')
        url   = (
            f'https://sdw-wsrest.ecb.europa.eu/service/data/{series_key}'
            f'?startPeriod={start}&format=csvdata'
        )
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                return None
            lines = r.text.strip().split('\n')
            # Find date and obs_value columns
            header = lines[0].split(',')
            date_idx = next((i for i, h in enumerate(header) if 'TIME_PERIOD' in h), None)
            val_idx  = next((i for i, h in enumerate(header) if 'OBS_VALUE'   in h), None)
            if date_idx is None or val_idx is None:
                return None
            records = {}
            for line in lines[1:]:
                parts = line.split(',')
                if len(parts) > max(date_idx, val_idx):
                    try:
                        records[pd.Timestamp(parts[date_idx])] = float(parts[val_idx])
                    except ValueError:
                        pass
            if records:
                return pd.Series(records).sort_index()
        except Exception as e:
            logger.debug(f'[ECB] {series_key}: {e}')
        return None

    def _boe_series(self, maturity: int = 10) -> Optional[pd.Series]:
        """Fetch UK gilt yields from Bank of England API."""
        # BoE series: IUDMNPY (10Y nominal)
        series_map = {10: 'IUDMNPY', 2: 'IUDMNY2', 5: 'IUDMNY5'}
        series_id  = series_map.get(maturity, 'IUDMNPY')
        start = (datetime.now() - timedelta(days=self.history_days)).strftime('%d/%b/%Y')
        url   = (
            f'https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp'
            f'?Travel=NIxRSxSUx&FromSeries=1&ToSeries=50&DAT=RNG'
            f'&FD=1&FM=Jan&FY=2020&TD=31&TM=Dec&TY=2099'
            f'&VFD=Y&html.x=66&html.y=26&C={series_id}&Filter=N'
        )
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and 'DATE' in r.text:
                lines  = r.text.strip().split('\n')
                header = lines[0].split(',')
                date_i = header.index('DATE') if 'DATE' in header else 0
                val_i  = 1
                records = {}
                for line in lines[1:]:
                    parts = line.split(',')
                    if len(parts) >= 2:
                        try:
                            records[pd.Timestamp(parts[date_i])] = float(parts[val_i])
                        except ValueError:
                            pass
                if records:
                    return pd.Series(records).sort_index()
        except Exception as e:
            logger.debug(f'[BoE] {series_id}: {e}')
        return None

    # ------------------------------------------------------------------
    def fetch_all(self) -> bool:
        """Fetch toutes les séries et remplir le cache."""
        logger.info('[YIELDS] Fetching sovereign yields…')
        cache: Dict[str, Dict[int, pd.Series]] = {}

        # ── USA ─────────────────────────────────────────────────────
        cache['US'] = {}
        for mat, sid in self.FRED_US.items():
            s = self._fred_series(sid)
            if s is not None and len(s) > 20:
                cache['US'][mat] = s
                logger.info(f'  US {mat}Y : {len(s)} points  last={s.iloc[-1]:.3f}%')

        # ── Europe via ECB (daily), fallback FRED mensuel ────────────
        for country in ('DE', 'FR', 'IT'):
            cache[country] = {}
            ecb_mats = self.ECB_SERIES.get(country, {})
            for mat, ecb_key in ecb_mats.items():
                s = self._ecb_series(ecb_key)
                if s is not None and len(s) > 10:
                    cache[country][mat] = s
                    logger.info(f'  {country} {mat}Y (ECB): {len(s)} pts  last={s.iloc[-1]:.3f}%')
                else:
                    fred_key = self.FRED_INTL.get(country, {}).get(mat)
                    if fred_key:
                        s = self._fred_series(fred_key)
                        if s is not None and len(s) > 5:
                            cache[country][mat] = s
                            logger.info(f'  {country} {mat}Y (FRED): {len(s)} pts  last={s.iloc[-1]:.3f}%')

        # ── UK ───────────────────────────────────────────────────────
        cache['UK'] = {}
        s = self._boe_series(10)
        if s is not None and len(s) > 10:
            cache['UK'][10] = s
            logger.info(f'  UK 10Y (BoE): {len(s)} pts  last={s.iloc[-1]:.3f}%')
        else:
            s = self._fred_series('IRLTLT01GBM156N')
            if s is not None:
                cache['UK'][10] = s
                logger.info(f'  UK 10Y (FRED): {len(s)} pts  last={s.iloc[-1]:.3f}%')

        # ── Japan ────────────────────────────────────────────────────
        cache['JP'] = {}
        s = self._fred_series('IRLTLT01JPM156N')
        if s is not None and len(s) > 5:
            cache['JP'][10] = s
            logger.info(f'  JP 10Y (FRED): {len(s)} pts  last={s.iloc[-1]:.3f}%')

        self._cache      = cache
        self.last_update = datetime.now()
        return bool(cache.get('US'))

    # ------------------------------------------------------------------
    def latest(self, country: str, maturity: int) -> Optional[float]:
        """Dernier taux disponible pour un pays/maturité (en %)."""
        s = self._cache.get(country, {}).get(maturity)
        return float(s.iloc[-1]) if s is not None and len(s) > 0 else None

    def history(self, country: str, maturity: int) -> Optional[pd.Series]:
        """Série historique complète pour un pays/maturité."""
        return self._cache.get(country, {}).get(maturity)

# ==============================================================================
# YIELD CURVE BUILDER  (Nelson-Siegel par pays)
# ==============================================================================

class YieldCurveBuilder:
    """
    Construit une courbe Nelson-Siegel par pays à partir des points fetched.
    Utilisé par le BondPricer pour le pricing DCF.
    """

    def __init__(self, fetcher: SovereignYieldFetcher):
        self.fetcher   = fetcher
        self._params: Dict[str, Tuple] = {}   # {country: (b0,b1,b2,tau)}

    @staticmethod
    def _ns(t: float, b0: float, b1: float, b2: float, tau: float) -> float:
        if t <= 0:
            return b0 + b1
        decay = (1 - np.exp(-t / tau)) / (t / tau)
        hump  = decay - np.exp(-t / tau)
        return b0 + b1 * decay + b2 * hump

    def fit(self, country: str):
        """Fitter NS sur les points disponibles pour ce pays."""
        points = {
            mat: self.fetcher.latest(country, mat)
            for mat in (1, 2, 5, 10, 30)
            if self.fetcher.latest(country, mat) is not None
        }
        if len(points) < 2:
            self._params[country] = (4.0, -0.5, 1.0, 2.0)
            return

        mats = np.array(list(points.keys()), dtype=float)
        obs  = np.array(list(points.values()), dtype=float)

        def objective(p):
            b0, b1, b2, tau = p
            if tau <= 0 or b0 <= 0:
                return 1e10
            fitted = np.array([self._ns(t, b0, b1, b2, tau) for t in mats])
            return np.sum((fitted - obs) ** 2)

        x0     = [obs.mean(), obs[0] - obs[-1], 0.5, 2.0]
        bounds = [(0.01, 20), (-15, 15), (-15, 15), (0.1, 30)]
        res    = minimize(objective, x0, bounds=bounds, method='L-BFGS-B')
        self._params[country] = tuple(res.x) if res.success else (obs.mean(), -0.3, 0.5, 2.0)

    def fit_all(self):
        for c in ('US', 'DE', 'FR', 'IT', 'UK', 'JP'):
            self.fit(c)

    def get_rate(self, country: str, maturity_years: float) -> float:
        """Taux (%) pour un pays et une maturité via NS."""
        if country not in self._params:
            self.fit(country)
        return self._ns(maturity_years, *self._params[country])


# ==============================================================================
# BOND PRICER  (DCF + Duration + Convexity)
# ==============================================================================

class BondPricer:
    """
    Pricing DCF complet d'une obligation souveraine.

    Rôle dans la stratégie :
    • Calcule le prix THÉORIQUE d'un futures (prix fair value)
    • Compare au prix MARCHÉ → alpha NPV
    • Fournit Duration / Convexity / DV01 pour le sizing et le hedging
    • Si alpha NPV va dans le même sens que le spread signal → confidence boostée
    """

    def __init__(self, curve: YieldCurveBuilder):
        self.curve = curve

    def price_bond(
        self,
        country: str,
        coupon_rate: float,          # annuel, décimal (ex: 0.043)
        par: float,                  # valeur nominale (ex: 100)
        years_to_maturity: float,
        freq: int = 2,               # semi-annuel par défaut
    ) -> Dict:
        """
        DCF complet : chaque flux est actualisé au taux spot NS correspondant.
        Retourne prix, duration modifiée, convexité, DV01.
        """
        coupon_pmt = par * coupon_rate / freq
        n          = max(1, int(round(years_to_maturity * freq)))
        pv = dur_num = conv_num = 0.0

        for i in range(1, n + 1):
            t   = i / freq
            rf  = self.curve.get_rate(country, t) / 100
            df  = 1 / (1 + rf / freq) ** i
            cf  = coupon_pmt + (par if i == n else 0)
            pv_cf     = cf * df
            pv        += pv_cf
            dur_num   += t * pv_cf
            conv_num  += t * (t + 1 / freq) * pv_cf

        if pv == 0:
            return {'price': par, 'modified_duration': 0, 'convexity': 0, 'dv01': 0}

        mid_rf  = self.curve.get_rate(country, years_to_maturity / 2) / 100
        mac_dur = dur_num / pv
        mod_dur = mac_dur / (1 + mid_rf / freq)
        convex  = conv_num / (pv * (1 + mid_rf / freq) ** 2)

        return {
            'price':             pv,
            'macaulay_duration': mac_dur,
            'modified_duration': mod_dur,
            'convexity':         convex,
            'dv01':              mod_dur * pv * 0.0001,
        }

    def npv_alpha(
        self,
        country: str,
        coupon_rate: float,
        par: float,
        years_to_maturity: float,
        market_price: float,
    ) -> Dict:
        """
        Alpha NPV = (Prix_marché − Prix_fair) / Prix_fair  en %
        Positif → bond cher (candidat SELL)
        Négatif → bond pas cher (candidat BUY)
        """
        fair    = self.price_bond(country, coupon_rate, par, years_to_maturity)
        alpha   = market_price - fair['price']
        alpha_pct = alpha / fair['price'] * 100

        return {
            'fair_price':  fair['price'],
            'alpha':       alpha,
            'alpha_pct':   alpha_pct,
            'alpha_bps':   alpha / fair['price'] * 10_000,
            **fair,
        }

# ==============================================================================
# SPREAD ANALYZER  (Signal 1 — Inter-pays)
# ==============================================================================

class SpreadAnalyzer:
    """
    Calcule les spreads entre pays et détecte les déviations via z-score.

    Logique :
    ─────────
    1. spread(t) = yield_A(t) − yield_B(t)
    2. z(t) = (spread(t) − mean(spread, window)) / std(spread, window)
    3. |z| > ZSCORE_ENTRY  → signal de mean-reversion
       |z| < ZSCORE_EXIT   → sortie de position

    Interprétation du signal :
    • z >> 0  : spread A−B trop large vs historique
                → A trop cher / B trop bon marché
                → SHORT futures A  +  LONG futures B

    • z << 0  : spread A−B trop étroit vs historique
                → A trop bon marché / B trop cher
                → LONG futures A  +  SHORT futures B
    """

    def __init__(
        self,
        fetcher: SovereignYieldFetcher,
        window: int  = ZSCORE_WINDOW,
        z_entry: float = ZSCORE_ENTRY,
        z_exit:  float = ZSCORE_EXIT,
    ):
        self.fetcher  = fetcher
        self.window   = window
        self.z_entry  = z_entry
        self.z_exit   = z_exit

    # ------------------------------------------------------------------
    def compute_spread_series(
        self, country_a: str, country_b: str, maturity: int
    ) -> Optional[pd.Series]:
        """Série historique du spread A−B pour une maturité donnée."""
        sa = self.fetcher.history(country_a, maturity)
        sb = self.fetcher.history(country_b, maturity)
        if sa is None or sb is None:
            return None

        # Aligner sur l'index commun (inner join)
        df = pd.DataFrame({'a': sa, 'b': sb}).dropna()
        if len(df) < 20:
            return None

        return df['a'] - df['b']

    # ------------------------------------------------------------------
    def zscore(self, series: pd.Series) -> float:
        """Z-score du dernier point vs fenêtre glissante."""
        s = series.dropna()
        if len(s) < 10:
            return 0.0
        window = min(self.window, len(s))
        mu     = s.iloc[-window:].mean()
        sigma  = s.iloc[-window:].std()
        if sigma < 1e-8:
            return 0.0
        return float((s.iloc[-1] - mu) / sigma)

    # ------------------------------------------------------------------
    def analyze_pair(
        self, country_a: str, country_b: str, maturity: int
    ) -> Dict:
        """
        Analyse complète d'une paire de spread.
        Retourne le signal, le z-score, le spread actuel et la confidence.
        """
        spread_series = self.compute_spread_series(country_a, country_b, maturity)

        label = f'{country_a}{maturity}Y-{country_b}{maturity}Y'

        if spread_series is None or len(spread_series) < 10:
            return {
                'pair':           label,
                'signal':         'NO_DATA',
                'z_score':        0.0,
                'spread_current': None,
                'spread_mean':    None,
                'spread_std':     None,
                'confidence':     0.0,
                'leg_long':       None,
                'leg_short':      None,
            }

        z       = self.zscore(spread_series)
        window  = min(self.window, len(spread_series))
        mu      = float(spread_series.iloc[-window:].mean())
        sigma   = float(spread_series.iloc[-window:].std())
        current = float(spread_series.iloc[-1])

        # ── Signal ────────────────────────────────────────────────────
        if z > self.z_entry:
            # Spread trop large → A cher, B bon marché → SHORT A / LONG B
            signal    = 'SPREAD_SHORT_A'
            leg_long  = country_b
            leg_short = country_a
        elif z < -self.z_entry:
            # Spread trop étroit → A bon marché, B cher → LONG A / SHORT B
            signal    = 'SPREAD_LONG_A'
            leg_long  = country_a
            leg_short = country_b
        else:
            signal    = 'HOLD'
            leg_long  = None
            leg_short = None

        # ── Confidence : 0 à 100 ──────────────────────────────────────
        # 50% du score vient de l'amplitude du z-score (capped à z=4)
        # 50% vient du nombre de points historiques (confiance statistique)
        z_score_pts  = min(abs(z) / 4.0 * 50.0, 50.0)
        history_pts  = min(len(spread_series) / self.window * 50.0, 50.0)
        confidence   = z_score_pts + history_pts

        return {
            'pair':           label,
            'country_a':      country_a,
            'country_b':      country_b,
            'maturity':       maturity,
            'signal':         signal,
            'z_score':        z,
            'spread_current': current,
            'spread_mean':    mu,
            'spread_std':     sigma,
            'deviation_bps':  (current - mu) * 100,   # en bps
            'confidence':     confidence,
            'leg_long':       leg_long,
            'leg_short':      leg_short,
            'n_points':       len(spread_series),
        }

    # ------------------------------------------------------------------
    def analyze_all(self) -> List[Dict]:
        """Analyser toutes les paires configurées dans SPREAD_PAIRS."""
        results = []
        for country_a, country_b, maturity in SPREAD_PAIRS:
            res = self.analyze_pair(country_a, country_b, maturity)
            results.append(res)
            if res['signal'] != 'HOLD' and res['signal'] != 'NO_DATA':
                logger.info(
                    f'[SPREAD] {res["pair"]:16s} | '
                    f'z={res["z_score"]:+5.2f} | '
                    f'spread={res["spread_current"]:+6.2f}% '
                    f'(moy={res["spread_mean"]:+5.2f}% ±{res["spread_std"]:.2f}%) | '
                    f'dev={res["deviation_bps"]:+5.1f}bps | '
                    f'{res["signal"]:20s} conf={res["confidence"]:.0f}%'
                )
        return results

# ==============================================================================
# SIGNAL AGGREGATOR  (Combine Spread + NPV)
# ==============================================================================

class SignalAggregator:
    """
    Combine le signal spread inter-pays (Signal 1) avec la confirmation NPV (Signal 2).

    Règle de combinaison :
    ──────────────────────
    • Signal spread seul, sans confirmation NPV → confidence × 0.80
    • Signal spread + NPV dans le même sens     → confidence × 1.20 (boost)
    • Signal spread + NPV opposé               → confidence × 0.60 (réduction)

    Exemple :
      Spread dit SHORT ZN (US cher vs Bund)
      NPV dit   ZN est 0.08% au-dessus du fair value
      → NPV CONFIRME le short ZN → boost confidence
    """

    def __init__(self, pricer: BondPricer):
        self.pricer = pricer

    # Paramètres de chaque futures pour le pricing NPV
    # (country, coupon_approx, par, années_maturité_nominale)
    FUTURES_PARAMS: Dict[str, Tuple] = {
        'ZT':   ('US', 0.045,  100,  2.0),
        'ZF':   ('US', 0.042,  100,  5.0),
        'ZN':   ('US', 0.043,  100, 10.0),
        'ZB':   ('US', 0.045,  100, 30.0),
        'FGBS': ('DE', 0.010,  100,  2.0),
        'FGBM': ('DE', 0.010,  100,  5.0),
        'FGBL': ('DE', 0.020,  100, 10.0),
        'FGBX': ('DE', 0.025,  100, 30.0),
        'R':    ('UK', 0.035,  100, 10.0),
    }

    # Mapping pays+maturité → futures symbol (legs)
    COUNTRY_TO_FUTURES: Dict[Tuple, str] = {
        ('US',  2):  'ZT',
        ('US',  5):  'ZF',
        ('US', 10):  'ZN',
        ('US', 30):  'ZB',
        ('DE',  2):  'FGBS',
        ('DE',  5):  'FGBM',
        ('DE', 10):  'FGBL',
        ('DE', 30):  'FGBX',
        ('UK', 10):  'R',
    }

    # Exchanges par futures
    FUTURES_EXCHANGE: Dict[str, str] = {
        'ZT': 'CBOT', 'ZF': 'CBOT', 'ZN': 'CBOT', 'ZB': 'CBOT',
        'FGBS': 'EUREX', 'FGBM': 'EUREX', 'FGBL': 'EUREX', 'FGBX': 'EUREX',
        'R': 'LIFFE',
    }

    def npv_confirmation(
        self,
        futures_symbol: str,
        market_price: Optional[float],
        spread_signal: str,
        leg_role: str,   # 'long' ou 'short'
    ) -> Dict:
        """
        Calcule l'alpha NPV et détermine si il confirme ou contredit le spread signal.

        spread_signal : 'SPREAD_SHORT_A' ou 'SPREAD_LONG_A'
        leg_role      : rôle de ce futures dans le trade ('long' ou 'short')
        """
        if futures_symbol not in self.FUTURES_PARAMS or market_price is None:
            return {'confirmed': False, 'alpha_pct': 0.0, 'multiplier': 1.0}

        country, coupon, par, years = self.FUTURES_PARAMS[futures_symbol]
        npv = self.pricer.npv_alpha(country, coupon, par, years, market_price)
        alpha_pct = npv['alpha_pct']

        # Logique de confirmation :
        # Si on est LONG ce futures → on veut qu'il soit bon marché (alpha_pct < 0)
        # Si on est SHORT ce futures → on veut qu'il soit cher (alpha_pct > 0)
        confirmed = (
            (leg_role == 'long'  and alpha_pct < -NPV_THRESHOLD) or
            (leg_role == 'short' and alpha_pct >  NPV_THRESHOLD)
        )
        opposed   = (
            (leg_role == 'long'  and alpha_pct >  NPV_THRESHOLD) or
            (leg_role == 'short' and alpha_pct < -NPV_THRESHOLD)
        )

        if confirmed:
            multiplier = 1.20
        elif opposed:
            multiplier = 0.60
        else:
            multiplier = 0.80

        return {
            'confirmed':  confirmed,
            'alpha_pct':  alpha_pct,
            'alpha_bps':  npv['alpha_bps'],
            'fair_price': npv['fair_price'],
            'mod_dur':    npv['modified_duration'],
            'convexity':  npv['convexity'],
            'dv01':       npv['dv01'],
            'multiplier': multiplier,
        }

    def build_trade(
        self,
        spread_result: Dict,
        prices: Dict[str, float],   # {futures_symbol: market_price}
    ) -> Optional[Dict]:
        """
        Construit le trade complet à partir du signal spread + confirmation NPV.
        Retourne None si données insuffisantes.
        """
        if spread_result['signal'] in ('HOLD', 'NO_DATA'):
            return None

        country_a = spread_result['country_a']
        country_b = spread_result['country_b']
        maturity  = spread_result['maturity']
        signal    = spread_result['signal']

        # Identifier les legs
        if signal == 'SPREAD_SHORT_A':
            leg_long_country  = country_b
            leg_short_country = country_a
        else:
            leg_long_country  = country_a
            leg_short_country = country_b

        fut_long  = self.COUNTRY_TO_FUTURES.get((leg_long_country,  maturity))
        fut_short = self.COUNTRY_TO_FUTURES.get((leg_short_country, maturity))

        if not fut_long or not fut_short:
            return None

        price_long  = prices.get(fut_long)
        price_short = prices.get(fut_short)

        # Confirmation NPV sur les deux legs
        npv_long  = self.npv_confirmation(fut_long,  price_long,  signal, 'long')
        npv_short = self.npv_confirmation(fut_short, price_short, signal, 'short')

        # Confidence finale : spread confidence × moyenne des multiplicateurs NPV
        npv_mult     = (npv_long['multiplier'] + npv_short['multiplier']) / 2
        final_conf   = min(spread_result['confidence'] * npv_mult, 100.0)

        return {
            'pair':          spread_result['pair'],
            'signal':        signal,
            'z_score':       spread_result['z_score'],
            'spread_current':spread_result['spread_current'],
            'spread_mean':   spread_result['spread_mean'],
            'deviation_bps': spread_result['deviation_bps'],
            'confidence':    final_conf,
            'leg_long':  {
                'futures':    fut_long,
                'exchange':   self.FUTURES_EXCHANGE.get(fut_long, 'SMART'),
                'price':      price_long,
                'npv':        npv_long,
            },
            'leg_short': {
                'futures':    fut_short,
                'exchange':   self.FUTURES_EXCHANGE.get(fut_short, 'SMART'),
                'price':      price_short,
                'npv':        npv_short,
            },
        }

# ==============================================================================
# POSITION SIZER
# ==============================================================================

class DynamicPositionSizer:
    """Sizing linéaire entre min et max selon la confidence (0-100)."""

    def __init__(
        self,
        min_pct:  float = MIN_POSITION_PCT,
        max_pct:  float = MAX_POSITION_PCT,
        min_conf: float = MIN_CONFIDENCE,
    ):
        self.min_pct  = min_pct
        self.max_pct  = max_pct
        self.min_conf = min_conf

    def calculate(self, equity: float, confidence: float, price: float) -> Dict:
        if confidence < self.min_conf or equity <= 0 or price <= 0:
            return {'can_trade': False, 'reason': f'conf={confidence:.0f}%', 'quantity': 0}

        norm  = max(0.0, min(1.0, (confidence - self.min_conf) / (100.0 - self.min_conf)))
        pct   = self.min_pct + (self.max_pct - self.min_pct) * norm
        value = equity * pct / 100.0
        qty   = max(1, int(value / (price * 1_000)))

        return {
            'can_trade': True,
            'quantity':  qty,
            'pct':       pct,
            'value':     qty * price * 1_000,
        }


# ==============================================================================
# MAIN BOT — IB Integration
# ==============================================================================

class GovernmentBondsBot:
    """
    Bot principal — orchestre toute la stratégie et exécute sur IBKR.

    Cycle complet à chaque scan :
    1. Refresh des taux souverains (toutes les 10 scans)
    2. Analyse spread inter-pays sur toutes les paires → Signal 1
    3. Fetch prix futures depuis IBKR → Signal 2 (NPV confirmation)
    4. Agrégation des deux signaux → confidence finale
    5. Position sizing + exécution
    """

    def __init__(self, ib_host: str = IB_HOST, ib_port: int = IB_PORT):
        self.ib   = IB()
        self.host = ib_host
        self.port = ib_port

        # Pipeline
        self.fetcher    = SovereignYieldFetcher(history_days=400)
        self.curve      = YieldCurveBuilder(self.fetcher)
        self.pricer     = BondPricer(self.curve)
        self.spread_ana = SpreadAnalyzer(self.fetcher)
        self.aggregator = SignalAggregator(self.pricer)
        self.sizer      = DynamicPositionSizer()

        self.equity  = 0.0
        self.running = False

    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        try:
            await self.ib.connectAsync(self.host, self.port, clientId=3)
            logger.info(f'[CONNECTED] {self.host}:{self.port}')
            await self._refresh_equity()
            return True
        except Exception as e:
            logger.error(f'[CONNECTION FAILED] {e}')
            return False

    async def _refresh_equity(self):
        try:
            for av in self.ib.accountValues():
                if av.tag == 'NetLiquidation' and av.currency == 'USD':
                    self.equity = float(av.value)
                    logger.info(f'[EQUITY] ${self.equity:,.0f}')
                    return
        except Exception:
            pass
        self.equity = 1_000_000.0

    # ------------------------------------------------------------------
    async def _front_month(self, symbol: str, exchange: str) -> Optional[Tuple]:
        """Retourne (contract, expiry_date) pour le front-month."""
        try:
            raw     = Future(symbol=symbol, exchange=exchange, currency='USD')
            details = await self.ib.reqContractDetailsAsync(raw)
            if not details:
                return None
            details.sort(key=lambda d: d.contract.lastTradeDateOrContractMonth)
            front  = details[0].contract
            expiry = datetime.strptime(front.lastTradeDateOrContractMonth[:8], '%Y%m%d')
            if (expiry - datetime.now()).days < 7 and len(details) > 1:
                front  = details[1].contract
                expiry = datetime.strptime(front.lastTradeDateOrContractMonth[:8], '%Y%m%d')
            return front, expiry
        except Exception as e:
            logger.debug(f'[CONTRACT] {symbol}: {e}')
            return None

    async def _live_price(self, contract) -> Optional[float]:
        """Prix live ou delayed depuis IB."""
        try:
            self.ib.reqMktData(contract, '', False, False)
            await asyncio.sleep(2)
            t = self.ib.ticker(contract)
            for v in [t.last, t.close,
                      (t.bid + t.ask) / 2 if t.bid and t.ask and t.bid > 0 else None]:
                if v and not np.isnan(v) and v > 0:
                    self.ib.cancelMktData(contract)
                    return float(v)
            self.ib.cancelMktData(contract)
        except Exception:
            pass
        return None

    async def _fetch_all_prices(self) -> Dict[str, Optional[float]]:
        """Fetch les prix de tous les futures du mapping."""
        prices: Dict[str, Optional[float]] = {}
        for (country, mat), symbol in self.aggregator.COUNTRY_TO_FUTURES.items():
            if symbol in prices:
                continue
            exchange = self.aggregator.FUTURES_EXCHANGE.get(symbol, 'SMART')
            result   = await self._front_month(symbol, exchange)
            if result:
                contract, _ = result
                price = await self._live_price(contract)
                prices[symbol] = price
                if price:
                    logger.info(f'  {symbol:6s} ({exchange}): {price:.3f}')
            else:
                prices[symbol] = None
            await asyncio.sleep(0.3)
        return prices

    async def _execute_leg(
        self,
        leg: Dict,
        action: str,
        sizing: Dict,
    ):
        """Exécuter un leg (BUY ou SELL) via IBKR."""
        symbol   = leg['futures']
        exchange = leg['exchange']
        price    = leg['price']
        qty      = sizing['quantity']

        if price is None:
            logger.warning(f'[SKIP] {symbol} — pas de prix marché')
            return

        result = await self._front_month(symbol, exchange)
        if not result:
            logger.warning(f'[SKIP] {symbol} — contrat non trouvé')
            return
        contract, _ = result
        limit = price * (1.001 if action == 'BUY' else 0.999)

        if DRY_RUN:
            logger.info(
                f'[DRY RUN] {action:4s} {qty}x {symbol} @ {limit:.3f}  '
                f'(NPV alpha: {leg["npv"]["alpha_pct"]:+.3f}%  '
                f'dur={leg["npv"].get("mod_dur", 0):.2f}  '
                f'dv01=${leg["npv"].get("dv01", 0):.2f})'
            )
            return

        order = LimitOrder(action, qty, limit)
        self.ib.placeOrder(contract, order)
        logger.info(f'[ORDER] {action} {qty}x {symbol} @ {limit:.3f}')

    # ------------------------------------------------------------------
    async def run(self):
        """Boucle principale."""
        self.running = True
        scan = 0

        logger.info('\n' + '='*80)
        logger.info('[START] Government Bond Arbitrage — Spread Inter-pays + NPV')
        logger.info(f'  Z-score entry  : ±{ZSCORE_ENTRY}')
        logger.info(f'  Z-score exit   : ±{ZSCORE_EXIT}')
        logger.info(f'  NPV threshold  : {NPV_THRESHOLD}%')
        logger.info(f'  Scan interval  : {SCAN_INTERVAL}s')
        logger.info(f'  Min confidence : {MIN_CONFIDENCE}%')
        logger.info(f'  Dry run        : {DRY_RUN}')
        logger.info('='*80)

        # Chargement initial
        self.fetcher.fetch_all()
        self.curve.fit_all()

        try:
            while self.running:
                scan += 1
                logger.info(f'\n{"="*80}')
                logger.info(f'[SCAN #{scan}] {datetime.now():%Y-%m-%d %H:%M:%S}')
                logger.info('='*80)

                await self._refresh_equity()

                # Refresh yields toutes les 10 scans
                if scan % 10 == 1 and scan > 1:
                    logger.info('[REFRESH] Mise à jour des taux souverains…')
                    self.fetcher.fetch_all()
                    self.curve.fit_all()

                # ── Signal 1 : Spread analysis ─────────────────────
                logger.info('\n[STEP 1] Spread Inter-pays')
                spread_results = self.spread_ana.analyze_all()

                # ── Signal 2 : Prix futures pour NPV ───────────────
                logger.info('\n[STEP 2] Fetch prix futures (NPV confirmation)')
                prices = await self._fetch_all_prices()

                # ── Agrégation & Exécution ──────────────────────────
                logger.info('\n[STEP 3] Agrégation + Exécution')
                for sr in spread_results:
                    trade = self.aggregator.build_trade(sr, prices)
                    if trade is None:
                        continue

                    logger.info(
                        f'\n  TRADE │ {trade["pair"]:18s} │ {trade["signal"]:20s}\n'
                        f'        │ z={trade["z_score"]:+5.2f} │ dev={trade["deviation_bps"]:+5.1f}bps │ conf={trade["confidence"]:.0f}%\n'
                        f'        │ LONG  {trade["leg_long"]["futures"]:6s} @ {trade["leg_long"]["price"] or "N/A"!s}  '
                        f'NPV alpha={trade["leg_long"]["npv"]["alpha_pct"]:+.3f}%\n'
                        f'        │ SHORT {trade["leg_short"]["futures"]:6s} @ {trade["leg_short"]["price"] or "N/A"!s}  '
                        f'NPV alpha={trade["leg_short"]["npv"]["alpha_pct"]:+.3f}%'
                    )

                    if trade['confidence'] < MIN_CONFIDENCE:
                        logger.info(f'        │ [SKIP] Confidence trop basse ({trade["confidence"]:.0f}%)')
                        continue

                    # Sizing basé sur le leg long (référence)
                    ref_price = trade['leg_long']['price'] or 100.0
                    sizing    = self.sizer.calculate(self.equity, trade['confidence'], ref_price)

                    if not sizing['can_trade']:
                        logger.info(f'        │ [SKIP] {sizing["reason"]}')
                        continue

                    # Exécuter les deux legs
                    await self._execute_leg(trade['leg_long'],  'BUY',  sizing)
                    await self._execute_leg(trade['leg_short'], 'SELL', sizing)

                logger.info(f'\n[SLEEP] Prochain scan dans {SCAN_INTERVAL}s…')
                await asyncio.sleep(SCAN_INTERVAL)

        except asyncio.CancelledError:
            logger.info('[STOPPED]')
        except Exception as e:
            logger.error(f'[CRASH] {e}')
            import traceback; traceback.print_exc()
        finally:
            self.running = False
            self.ib.disconnect()
            logger.info('[DISCONNECTED]')


# ==============================================================================
# ENTRY POINT
# ==============================================================================

async def main():
    bot = GovernmentBondsBot(ib_host=IB_HOST, ib_port=IB_PORT)

    if not await bot.connect():
        logger.error('[FAILED] Connexion IBKR impossible')
        logger.info('  1. TWS ouvert ?')
        logger.info('  2. API activée ? → File → Global Config → API')
        logger.info('  3. Port : 7497 = Paper | 7496 = Live')
        return

    try:
        await bot.run()
    except KeyboardInterrupt:
        bot.running = False
        logger.info('[SHUTDOWN] Ctrl+C')


if __name__ == '__main__':
    asyncio.run(main())
