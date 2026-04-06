"""
================================================================================
Bond Arbitrage Strategy — T-Bills & Corporate Bonds
================================================================================

Stratégie d'arbitrage obligataire en 4 étapes :

  STEP 1 — Theoretical Pricing
      - Yield curve Nelson-Siegel (FRED live data)
      - Full DCF pricing
      - Modified Duration & Convexity

  STEP 2 — Credit Risk Adjustment  [Enhanced Modigliani-Miller]
      - Altman Z-Score (early warning)
      - Merton structural model → PD (iterative solver)
      - LGD estimation (sector + leverage + asset coverage)
      - Credit spread = PD × LGD / (1 − PD)
      - APV = VU + PV(tax shield) − PV(distress) − PV(agency)

  STEP 3 — Equity Cross-Check
      - Bond-implied PD vs Merton PD from equity
      - Equity-implied spread vs market spread
      - Detect pricing inconsistency between bond & equity markets

  STEP 4 — Mispricing Detection
      - Alpha = Price_market − Price_fair
      - Signal generation + dynamic position sizing

================================================================================
"""

# ==============================================================================
# CONFIGURATION
# ==============================================================================

IB_HOST            = '127.0.0.1'
IB_PORT            = 7497        # 7497 = Paper, 7496 = Live
SCAN_INTERVAL      = 60          # seconds between full scans
MIN_ALPHA_BPS      = 20          # min mispricing in basis points to trade
MIN_CONFIDENCE     = 25.0        # min confidence score (0-100) to trade
MIN_POSITION_PCT   = 1.0         # min position size (% of equity)
MAX_POSITION_PCT   = 5.0         # max position size (% of equity)
DRY_RUN            = True        # True = log orders only, False = place real orders

# ==============================================================================
# IMPORTS
# ==============================================================================

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from scipy.stats import norm
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
# STEP 1 — YIELD CURVE & THEORETICAL PRICING
# ==============================================================================

class YieldCurveBuilder:
    """
    Nelson-Siegel yield curve bootstrapped from live FRED data.
    Covers the full maturity spectrum: 1M T-Bill → 30Y T-Bond.
    """

    # FRED series IDs by maturity (years)
    FRED_SERIES: Dict[float, str] = {
        0.083: 'DTB4WK',   # 1-Month T-Bill
        0.25:  'DTB3',     # 3-Month T-Bill
        0.5:   'DTB6',     # 6-Month T-Bill
        1.0:   'DGS1',
        2.0:   'DGS2',
        3.0:   'DGS3',
        5.0:   'DGS5',
        7.0:   'DGS7',
        10.0:  'DGS10',
        20.0:  'DGS20',
        30.0:  'DGS30',
    }

    def __init__(self):
        self.yields: Dict[float, float] = {}
        self.ns_params: Tuple = (4.5, -0.5, 1.0, 2.0)   # beta0, beta1, beta2, tau
        self.last_update: Optional[datetime] = None

    # ------------------------------------------------------------------
    def fetch_yields(self) -> bool:
        """Fetch live yields from FRED and fit Nelson-Siegel."""
        yields = {}
        for maturity, series_id in self.FRED_SERIES.items():
            try:
                url = (
                    f'https://fred.stlouisfed.org/graph/fredgraph.csv'
                    f'?id={series_id}&cosd=2024-01-01'
                )
                r = requests.get(url, timeout=6)
                if r.status_code == 200:
                    for line in reversed(r.text.strip().split('\n')[1:]):
                        parts = line.split(',')
                        if len(parts) == 2 and parts[1] not in ('.', ''):
                            yields[maturity] = float(parts[1])
                            break
            except Exception as e:
                logger.warning(f'[FRED] {series_id}: {e}')

        if len(yields) >= 4:
            self.yields = yields
            self.last_update = datetime.now()
            self._fit_nelson_siegel()
            logger.info(f'[YIELD CURVE] {len(yields)} points loaded, NS fitted')
            return True

        # Fallback
        logger.warning('[YIELD CURVE] Insufficient FRED data — using fallback')
        self.yields = {
            0.25: 5.25, 0.5: 5.15, 1.0: 4.90, 2.0: 4.50,
            5.0: 4.20, 10.0: 4.35, 30.0: 4.55
        }
        self._fit_nelson_siegel()
        return False

    # ------------------------------------------------------------------
    @staticmethod
    def _ns_formula(t: float, b0: float, b1: float, b2: float, tau: float) -> float:
        """Nelson-Siegel model: yield = f(maturity)."""
        if t <= 0:
            return b0 + b1
        decay = (1 - np.exp(-t / tau)) / (t / tau)
        hump  = decay - np.exp(-t / tau)
        return b0 + b1 * decay + b2 * hump

    def _fit_nelson_siegel(self):
        """Least-squares fit of NS parameters to observed yields."""
        mats = np.array(list(self.yields.keys()))
        obs  = np.array(list(self.yields.values()))

        def objective(params):
            b0, b1, b2, tau = params
            if tau <= 0 or b0 <= 0:
                return 1e10
            fitted = np.array([self._ns_formula(t, b0, b1, b2, tau) for t in mats])
            return np.sum((fitted - obs) ** 2)

        x0     = [obs[-1], obs[0] - obs[-1], 0.5, 2.0]
        bounds = [(0.1, 15), (-10, 10), (-10, 10), (0.1, 30)]
        result = minimize(objective, x0, bounds=bounds, method='L-BFGS-B')
        self.ns_params = tuple(result.x) if result.success else (4.5, -0.5, 1.0, 2.0)

    # ------------------------------------------------------------------
    def get_rate(self, maturity_years: float) -> float:
        """Return annualised yield (%) for a given maturity via NS model."""
        return self._ns_formula(maturity_years, *self.ns_params)

    def get_discount_factor(self, t: float) -> float:
        """Continuous-compounding discount factor e^(-r*t)."""
        return np.exp(-(self.get_rate(t) / 100) * t)


# ==============================================================================

class BondPricer:
    """
    Full DCF bond pricer.
    Works for T-Bills (zero-coupon, discount basis) and coupon bonds.
    Computes: price, modified duration, convexity, DV01.
    """

    def __init__(self, curve: YieldCurveBuilder):
        self.curve = curve

    # ------------------------------------------------------------------
    def price_bond(
        self,
        coupon_rate: float,        # annual rate, decimal (e.g. 0.045)
        par: float,                # face value
        years_to_maturity: float,
        credit_spread: float = 0.0,  # decimal (e.g. 0.015 = 150 bps)
        freq: int = 2              # payments per year (2 = semi-annual)
    ) -> Dict:
        """
        Price a bond via full DCF.
        Discount rate at each coupon date = spot risk-free rate + credit_spread.
        Returns price, modified duration, convexity, DV01.
        """
        coupon_pmt = (par * coupon_rate) / freq
        n          = max(1, int(round(years_to_maturity * freq)))

        pv = dur_num = conv_num = 0.0

        for i in range(1, n + 1):
            t  = i / freq
            rf = self.curve.get_rate(t) / 100
            dr = rf + credit_spread
            df = 1 / (1 + dr / freq) ** i
            cf = coupon_pmt + (par if i == n else 0)

            pv_cf     = cf * df
            pv        += pv_cf
            dur_num   += t * pv_cf
            conv_num  += t * (t + 1 / freq) * pv_cf

        if pv == 0:
            return {'price': par, 'macaulay_duration': 0,
                    'modified_duration': 0, 'convexity': 0, 'dv01': 0}

        mid_rf  = self.curve.get_rate(years_to_maturity / 2) / 100
        ytm_est = mid_rf + credit_spread
        mac_dur = dur_num / pv
        mod_dur = mac_dur / (1 + ytm_est / freq)
        convex  = conv_num / (pv * (1 + ytm_est / freq) ** 2)

        return {
            'price':              pv,
            'macaulay_duration':  mac_dur,
            'modified_duration':  mod_dur,
            'convexity':          convex,
            'dv01':               mod_dur * pv * 0.0001,   # $ per 1bp
        }

    # ------------------------------------------------------------------
    def ytm_to_price(
        self,
        ytm: float,
        coupon_rate: float,
        par: float,
        years: float,
        freq: int = 2
    ) -> float:
        """Convert a YTM (decimal) to a clean/dirty price."""
        coupon    = par * coupon_rate / freq
        n         = max(1, int(round(years * freq)))
        period_r  = ytm / freq
        if period_r == 0:
            return coupon * n + par
        pv_coupons = coupon * (1 - (1 + period_r) ** -n) / period_r
        pv_par     = par / (1 + period_r) ** n
        return pv_coupons + pv_par

    # ------------------------------------------------------------------
    def price_tbill(self, face: float, days: int, discount_rate: float) -> float:
        """T-Bill price on discount basis (bank discount convention)."""
        return face * (1 - discount_rate * days / 360)

# ==============================================================================
# STEP 2 — CREDIT RISK ADJUSTMENT  [Enhanced Modigliani-Miller]
# ==============================================================================

class EnhancedModiglianiMiller:
    """
    Enhanced APV framework for corporate bond credit risk.

    Improvements over the original MM_Options_Bot:
    ─────────────────────────────────────────────────
    • Merton model uses an iterative solver (not a single-pass estimate)
      to back out asset value V and asset volatility σ_V from equity data.
    • Altman Z-Score blended with Merton PD for robustness.
    • LGD calibrated by sector recovery rate, asset coverage and leverage.
    • Credit spread = PD_blended × LGD / (1 − PD_blended).
    • Tax shield discounted at cost-of-debt (Miles-Ezzell correction).
    • Distress costs scale with PD and asset value (Andrade-Kaplan 1998).
    • Agency costs from Jensen free-cash-flow overinvestment proxy.
    """

    # Base LGD (= 1 − recovery rate) by GICS sector
    SECTOR_LGD: Dict[str, float] = {
        'Technology':              0.55,
        'Healthcare':              0.50,
        'Financial Services':      0.45,
        'Energy':                  0.60,
        'Utilities':               0.35,
        'Consumer Cyclical':       0.60,
        'Consumer Defensive':      0.45,
        'Industrials':             0.55,
        'Materials':               0.60,
        'Real Estate':             0.50,
        'Communication Services':  0.55,
        'default':                 0.55,
    }

    def __init__(self, ticker: str, curve: YieldCurveBuilder):
        self.ticker = ticker
        self.curve  = curve
        self.rf     = curve.get_rate(1.0) / 100   # 1Y risk-free as base
        self.stock  = yf.Ticker(ticker)
        self.data: Dict = {}

    # ------------------------------------------------------------------
    def fetch_data(self) -> bool:
        """Pull financials from yfinance and compute derived ratios."""
        try:
            info = self.stock.info
            bs   = self.stock.balance_sheet
            inc  = self.stock.income_stmt
            cf   = self.stock.cashflow
            hist = self.stock.history(period='1y')

            def _safe(df, key, default=0.0):
                try:
                    val = df.loc[key].iloc[0]
                    return float(val) if pd.notna(val) else default
                except Exception:
                    return default

            mkt_cap   = float(info.get('marketCap', 0) or 0)
            price     = float(info.get('currentPrice', 0) or 0)
            shares    = float(info.get('sharesOutstanding', 0) or 0)
            total_dbt = _safe(bs, 'Total Debt')
            cash      = _safe(bs, 'Cash And Cash Equivalents')
            eq_book   = _safe(bs, 'Stockholders Equity')
            tot_assets= _safe(bs, 'Total Assets') or mkt_cap + total_dbt
            cur_assets= _safe(bs, 'Current Assets')
            cur_liab  = _safe(bs, 'Current Liabilities')
            ret_earn  = _safe(bs, 'Retained Earnings')
            ebit      = _safe(inc, 'EBIT')
            ebitda    = _safe(inc, 'EBITDA') or ebit * 1.15
            net_inc   = _safe(inc, 'Net Income')
            revenue   = _safe(inc, 'Total Revenue')
            int_exp   = abs(_safe(inc, 'Interest Expense')) or total_dbt * 0.05
            fcf_val   = _safe(cf, 'Free Cash Flow')

            # Annualised equity volatility from 1Y daily returns
            sigma_eq = 0.30
            if len(hist) > 20:
                rets     = hist['Close'].pct_change().dropna()
                sigma_eq = float(rets.std() * np.sqrt(252))

            self.data = {
                # Market
                'market_cap':       mkt_cap,
                'stock_price':      price,
                'shares':           shares,
                'enterprise_value': mkt_cap + total_dbt - cash,
                # Balance sheet
                'total_debt':       total_dbt,
                'cash':             cash,
                'net_debt':         total_dbt - cash,
                'eq_book':          eq_book,
                'total_assets':     tot_assets,
                'cur_assets':       cur_assets,
                'cur_liab':         cur_liab,
                'working_capital':  cur_assets - cur_liab,
                'retained_earnings':ret_earn,
                # P&L
                'ebit':             ebit,
                'ebitda':           ebitda,
                'net_income':       net_inc,
                'revenue':          revenue,
                'interest_expense': int_exp,
                'fcf':              fcf_val,
                'tax_rate':         float(info.get('effectiveTaxRate', 0.25) or 0.25),
                'beta':             float(info.get('beta', 1.0) or 1.0),
                # Derived
                'debt_to_equity':   total_dbt / eq_book if eq_book > 0 else 0,
                'interest_coverage':ebit / int_exp      if int_exp  > 0 else 999,
                'sigma_equity':     sigma_eq,
                'sector':           info.get('sector', 'default') or 'default',
            }
            return True

        except Exception as e:
            logger.debug(f'[MM] {self.ticker} fetch failed: {e}')
            return False

    # ------------------------------------------------------------------
    # Altman Z-Score  (Altman 1968, public-company version)
    # ------------------------------------------------------------------
    def altman_zscore(self) -> Dict:
        """
        Z = 1.2·X1 + 1.4·X2 + 3.3·X3 + 0.6·X4 + 1.0·X5

        Z > 2.99  → Safe    (PD proxy ~1–5%)
        1.81–2.99 → Grey    (PD proxy ~5–15%)
        Z < 1.81  → Distress (PD proxy 15–40%)
        """
        d  = self.data
        TA = d['total_assets']
        if TA <= 0:
            return {'z_score': 0.0, 'zone': 'Unknown', 'pd_proxy': 0.50}

        X1 = d['working_capital']  / TA
        X2 = d['retained_earnings']/ TA
        X3 = d['ebit']             / TA
        X4 = d['market_cap'] / d['total_debt'] if d['total_debt'] > 0 else 10.0
        X5 = d['revenue']          / TA

        z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5

        if z > 2.99:
            zone     = 'Safe'
            pd_proxy = max(0.005, 0.01 - (z - 2.99) * 0.005)
        elif z > 1.81:
            zone     = 'Grey'
            pd_proxy = 0.05 + (2.99 - z) / (2.99 - 1.81) * 0.10
        else:
            zone     = 'Distress'
            pd_proxy = min(0.40, 0.15 + (1.81 - z) * 0.05)

        return {'z_score': z, 'zone': zone, 'pd_proxy': float(pd_proxy)}

    # ------------------------------------------------------------------
    # Merton Structural Model — Iterative Solver
    # ------------------------------------------------------------------
    def merton_pd(self, horizon_years: float = 1.0) -> Dict:
        """
        Iterative Merton model to back out asset value V and σ_V
        from observed equity market cap E and equity volatility σ_E.

        Key equations:
          E = V·N(d1) − D·e^(−rf·T)·N(d2)      [Black-Scholes call]
          σ_E·E = N(d1)·σ_V·V                    [Ito's lemma]

        Returns PD = N(−d2) and distance-to-default d2.
        """
        E       = self.data['market_cap']
        D       = self.data['total_debt']
        sigma_E = self.data['sigma_equity']
        rf      = self.rf
        T       = horizon_years

        if E <= 0 or D <= 0 or sigma_E <= 0:
            return {'pd': 0.01, 'dd': 3.0, 'asset_value': E + D, 'asset_vol': sigma_E}

        # Seed estimates
        V       = E + D
        sigma_V = sigma_E * E / V

        for _ in range(200):
            if sigma_V <= 0:
                break
            d1 = (np.log(V / D) + (rf + 0.5 * sigma_V**2) * T) / (sigma_V * np.sqrt(T))
            d2 = d1 - sigma_V * np.sqrt(T)

            Nd1 = norm.cdf(d1)
            Nd2 = norm.cdf(d2)

            # New asset value from equity equation
            V_new = (E + D * np.exp(-rf * T) * Nd2) / Nd1 if Nd1 > 1e-8 else E + D
            # New asset vol from Ito relation
            sigma_V_new = sigma_E * E / (Nd1 * V_new) if (Nd1 * V_new) > 0 else sigma_E

            # Damped update for stability
            V       = 0.7 * V_new       + 0.3 * V
            sigma_V = 0.7 * sigma_V_new + 0.3 * sigma_V
            sigma_V = max(0.01, min(sigma_V, 2.0))

            if abs(V_new - V) / (V + 1e-8) < 1e-7:
                break

        d2_final = (np.log(V / D) + (rf - 0.5 * sigma_V**2) * T) / (sigma_V * np.sqrt(T))
        pd       = float(norm.cdf(-d2_final))

        return {
            'pd':          min(pd, 0.99),
            'dd':          d2_final,        # Distance-to-default
            'asset_value': V,
            'asset_vol':   sigma_V,
        }

    # ------------------------------------------------------------------
    # LGD Estimation
    # ------------------------------------------------------------------
    def estimate_lgd(self) -> float:
        """
        Base LGD from sector, adjusted for:
        • Asset coverage  (high coverage → lower LGD)
        • Financial leverage (high D/E → higher LGD)
        """
        sector   = self.data.get('sector', 'default')
        base_lgd = self.SECTOR_LGD.get(sector, self.SECTOR_LGD['default'])

        # Asset coverage adjustment  (coverage ratio = TA / D)
        TA       = self.data['total_assets']
        D        = self.data['total_debt']
        coverage = TA / D if D > 0 else 5.0
        cov_adj  = max(-0.15, min(0.15, (1.0 - coverage) * 0.10))

        # Leverage adjustment
        de         = self.data['debt_to_equity']
        lev_adj    = min(0.10, max(0.0, (de - 2.0) * 0.02))

        return float(min(0.90, max(0.10, base_lgd + cov_adj + lev_adj)))

    # ------------------------------------------------------------------
    # Credit Spread
    # ------------------------------------------------------------------
    def credit_spread(self, horizon_years: float = 5.0) -> Dict:
        """
        Actuarial credit spread: s = PD_blended × LGD / (1 − PD_blended)

        PD_blended = 70% Merton + 30% Altman proxy  (diversification of models)
        """
        merton  = self.merton_pd(horizon_years)
        z       = self.altman_zscore()
        lgd     = self.estimate_lgd()

        pd_blended = 0.70 * merton['pd'] + 0.30 * z['pd_proxy']
        spread     = pd_blended * lgd / max(1.0 - pd_blended, 0.01)

        # Implied rating bucket
        if   spread < 0.0050: rating = 'AAA/AA'
        elif spread < 0.0100: rating = 'A'
        elif spread < 0.0175: rating = 'BBB'
        elif spread < 0.0300: rating = 'BB'
        elif spread < 0.0500: rating = 'B'
        else:                 rating = 'CCC/D'

        return {
            'spread':               spread,
            'spread_bps':           spread * 10_000,
            'blended_pd':           pd_blended,
            'lgd':                  lgd,
            'merton_pd':            merton['pd'],
            'distance_to_default':  merton['dd'],
            'asset_value':          merton['asset_value'],
            'asset_vol':            merton['asset_vol'],
            'z_score':              z['z_score'],
            'z_zone':               z['zone'],
            'implied_rating':       rating,
        }

    # ------------------------------------------------------------------
    # APV — Adjusted Present Value  (full M-M decomposition)
    # ------------------------------------------------------------------
    def calculate_apv(self) -> Dict:
        """
        APV = VU + PV(tax shield) − PV(distress costs) − PV(agency costs)

        • Tax shield   : τ × interest_expense / k_d  (Miles-Ezzell: discounted at k_d)
        • Distress cost: PD × 20% × V_assets  (Andrade-Kaplan 1998)
        • Agency cost  : Jensen FCF proxy + overleverage penalty
        """
        d  = self.data
        cr = self.credit_spread()

        # ── Tax Shield ──────────────────────────────────────────────
        k_d         = self.rf + cr['spread']
        ts_annual   = d['tax_rate'] * d['interest_expense']
        pv_ts       = ts_annual / k_d if k_d > 0 else 0.0

        # ── Unlevered Value (strip tax shield from EV) ───────────────
        VU = d['enterprise_value'] - pv_ts

        # ── Financial Distress Costs ─────────────────────────────────
        pv_distress = cr['blended_pd'] * 0.20 * cr['asset_value']

        # ── Agency Costs (Jensen 1986) ───────────────────────────────
        agency_score = 0.0
        if d['debt_to_equity'] > 2.0:
            agency_score += (d['debt_to_equity'] - 2.0) * 0.03
        fcf_yield = abs(d['fcf']) / d['market_cap'] if d['market_cap'] > 0 else 0
        if fcf_yield > 0.10:
            agency_score += (fcf_yield - 0.10) * 0.30
        pv_agency = agency_score * d['market_cap']

        # ── Levered Value ────────────────────────────────────────────
        VL = VU + pv_ts - pv_distress - pv_agency

        target_price   = VL / d['shares'] if d['shares'] > 0 else 0.0
        divergence_pct = (VL - d['enterprise_value']) / d['enterprise_value'] * 100 \
                         if d['enterprise_value'] > 0 else 0.0

        return {
            'VU':              VU,
            'pv_tax_shield':   pv_ts,
            'pv_distress':     pv_distress,
            'pv_agency':       pv_agency,
            'VL_theoretical':  VL,
            'target_price':    target_price,
            'divergence_pct':  divergence_pct,
            'cost_of_debt':    k_d,
            **cr,
        }

# ==============================================================================
# STEP 3 — EQUITY CROSS-CHECK
# ==============================================================================

class EquityCrossCheck:
    """
    Detect pricing inconsistencies between the bond market and the equity market.

    M-M Insight
    ───────────
    Under M-M, both markets are pricing the same underlying firm.
    If they disagree on the firm's implied PD (or value), there is an arbitrage:

        Bond market  → implied PD via credit spread inversion
        Equity market → implied PD via Merton model on stock price/vol

    If  PD_bond >> PD_equity  → bond is *cheap*  (BUY_BOND)
    If  PD_bond << PD_equity  → bond is *expensive* (SELL_BOND / consider equity put)

    Alpha = bond_price_market − bond_price_fair
      where fair price uses equity-implied credit spread (from APV)
    """

    def __init__(self, pricer: BondPricer, mm: EnhancedModiglianiMiller):
        self.pricer = pricer
        self.mm     = mm

    # ------------------------------------------------------------------
    def cross_check(
        self,
        bond_market_ytm: float,      # observed YTM, decimal
        bond_coupon: float,          # bond coupon rate, decimal
        bond_par: float,             # face value
        bond_years: float,           # time to maturity in years
    ) -> Dict:
        """
        Full cross-check pipeline.

        Returns a dict with the alpha signal, basis-point gap,
        PD divergence, and all intermediate metrics.
        """
        d   = self.mm.data
        apv = self.mm.calculate_apv()

        # ── Risk-free component at this maturity ─────────────────────
        rf_rate     = self.mm.curve.get_rate(bond_years) / 100
        mkt_spread  = max(0.0, bond_market_ytm - rf_rate)

        # ── Bond price at market YTM ──────────────────────────────────
        bond_price_mkt  = self.pricer.ytm_to_price(
            bond_market_ytm, bond_coupon, bond_par, bond_years
        )

        # ── PD implied by the bond market (spread inversion) ─────────
        lgd             = self.mm.estimate_lgd()
        pd_from_bond    = mkt_spread / (lgd + mkt_spread) \
                          if (lgd + mkt_spread) > 0 else 0.0

        # ── PD implied by equity market (Merton) ─────────────────────
        merton          = self.mm.merton_pd(bond_years)
        pd_from_equity  = merton['pd']

        # ── PD Gap ───────────────────────────────────────────────────
        pd_gap = pd_from_bond - pd_from_equity   # >0 → bond mkt more fearful

        # ── Fair bond price using equity-implied spread ───────────────
        eq_spread       = apv['spread']
        fair_dict       = self.pricer.price_bond(
            coupon_rate=bond_coupon,
            par=bond_par,
            years_to_maturity=bond_years,
            credit_spread=eq_spread,
        )
        bond_price_fair = fair_dict['price']

        # ── Alpha ─────────────────────────────────────────────────────
        alpha_price     = bond_price_mkt - bond_price_fair
        alpha_bps       = (bond_market_ytm - (rf_rate + eq_spread)) * 10_000

        # ── Signal ────────────────────────────────────────────────────
        #   alpha_bps < 0 → market YTM > fair YTM → bond cheap → BUY
        #   alpha_bps > 0 → market YTM < fair YTM → bond rich  → SELL
        if   alpha_bps < -MIN_ALPHA_BPS:  signal = 'BUY_BOND'
        elif alpha_bps >  MIN_ALPHA_BPS:  signal = 'SELL_BOND'
        else:                              signal = 'HOLD'

        # ── Confidence (0–100) ────────────────────────────────────────
        pd_score    = min(abs(pd_gap) / 0.10 * 50.0, 50.0)
        price_score = min(abs(alpha_bps) / 100.0 * 50.0, 50.0)
        confidence  = pd_score + price_score

        return {
            'signal':                   signal,
            'alpha_bps':                alpha_bps,
            'alpha_price':              alpha_price,
            'bond_price_market':        bond_price_mkt,
            'bond_price_fair':          bond_price_fair,
            'modified_duration':        fair_dict['modified_duration'],
            'convexity':                fair_dict['convexity'],
            'dv01':                     fair_dict['dv01'],
            'pd_from_bond':             pd_from_bond,
            'pd_from_equity':           pd_from_equity,
            'pd_gap':                   pd_gap,
            'market_spread_bps':        mkt_spread  * 10_000,
            'equity_spread_bps':        eq_spread   * 10_000,
            'ev_from_equity':           d['market_cap'] + d['net_debt'],
            'confidence':               confidence,
            'apv':                      apv,
        }

# ==============================================================================
# STEP 4 — MISPRICING DETECTOR
# ==============================================================================

class MispricingDetector:
    """
    Aggregates signals from Steps 1–3 into a single Alpha per instrument.

      Alpha = Price_market − Price_fair

    Handles two asset classes:
    ─────────────────────────
    • Treasuries (T-Bills / T-Notes / T-Bonds)
        Pure rate arbitrage: DCF at NS curve, zero credit spread.
        Instruments traded via CME/CBOT futures (ZT, ZF, ZN, ZB).

    • Corporate bonds
        Credit-adjusted DCF + equity cross-check via Enhanced M-M.
    """

    def __init__(self, curve: YieldCurveBuilder, pricer: BondPricer):
        self.curve  = curve
        self.pricer = pricer

    # ------------------------------------------------------------------
    def analyze_treasury(
        self,
        coupon_rate: float,
        par: float,
        years_to_maturity: float,
        market_price: float,
        label: str = '',
    ) -> Dict:
        """
        Step 1 + Step 4 for a Treasury instrument.
        Fair price = full DCF at NS spot curve, no credit spread.
        """
        fair      = self.pricer.price_bond(coupon_rate, par, years_to_maturity, 0.0)
        alpha     = market_price - fair['price']
        alpha_bps = alpha / fair['price'] * 10_000

        if   alpha_bps < -2: signal = 'BUY'
        elif alpha_bps >  2: signal = 'SELL'
        else:                 signal = 'HOLD'

        return {
            'type':               'TREASURY',
            'label':              label,
            'market_price':       market_price,
            'fair_price':         fair['price'],
            'alpha':              alpha,
            'alpha_bps':          alpha_bps,
            'signal':             signal,
            'confidence':         min(abs(alpha_bps) * 5.0, 100.0),
            'modified_duration':  fair['modified_duration'],
            'convexity':          fair['convexity'],
            'dv01':               fair['dv01'],
        }

    # ------------------------------------------------------------------
    def analyze_corporate_bond(
        self,
        ticker: str,
        bond_coupon: float,
        bond_par: float,
        bond_years: float,
        bond_market_ytm: float,
    ) -> Optional[Dict]:
        """
        Steps 2 + 3 + 4 for a corporate bond.
        Returns None if financial data cannot be fetched.
        """
        mm = EnhancedModiglianiMiller(ticker, self.curve)
        if not mm.fetch_data():
            return None

        checker = EquityCrossCheck(self.pricer, mm)
        cross   = checker.cross_check(
            bond_market_ytm=bond_market_ytm,
            bond_coupon=bond_coupon,
            bond_par=bond_par,
            bond_years=bond_years,
        )

        logger.info(
            f'[{ticker:6s}] '
            f'Alpha: {cross["alpha_bps"]:+6.1f} bps | '
            f'PD bond: {cross["pd_from_bond"]*100:4.1f}% '
            f'vs equity: {cross["pd_from_equity"]*100:4.1f}% | '
            f'Rating: {cross["apv"]["implied_rating"]:7s} | '
            f'Z: {cross["apv"]["z_score"]:4.2f} ({cross["apv"]["z_zone"]}) | '
            f'Signal: {cross["signal"]} ({cross["confidence"]:.0f}%)'
        )

        return {'type': 'CORPORATE', 'ticker': ticker, **cross}


# ==============================================================================
# POSITION SIZER
# ==============================================================================

class DynamicPositionSizer:
    """
    Linear interpolation between min and max position sizes
    based on confidence score [MIN_CONFIDENCE, 100].
    """

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

        norm  = (confidence - self.min_conf) / (100.0 - self.min_conf)
        norm  = max(0.0, min(1.0, norm))
        pct   = self.min_pct + (self.max_pct - self.min_pct) * norm
        value = equity * pct / 100.0
        qty   = max(1, int(value / (price * 1_000)))   # 1 futures contract ≈ $1k face

        return {
            'can_trade': True,
            'quantity':  qty,
            'pct':       pct,
            'value':     qty * price * 1_000,
        }


# ==============================================================================
# MAIN BOT — IB Integration
# ==============================================================================

class BondsArbitrageBot:
    """
    Orchestration bot:
    • Scans Treasury futures (ZT / ZF / ZN / ZB) for rate arbitrage
    • Scans corporate bond watchlist for credit arbitrage
    • Connects to Interactive Brokers via ib_insync
    """

    # ── Treasury futures ─────────────────────────────────────────────
    TREASURY_SYMBOLS: List[Tuple[str, int, float]] = [
        # (IB symbol, nominal maturity years, approx coupon)
        ('ZT',  2,  0.045),
        ('ZF',  5,  0.042),
        ('ZN', 10,  0.043),
        ('ZB', 30,  0.045),
    ]

    # ── Corporate bond watchlist ──────────────────────────────────────
    # (equity ticker, coupon, par, years_to_maturity, market_ytm or None)
    # When market_ytm is None, it defaults to rf + 150 bps placeholder.
    # In production, replace with live bond data from IB or Bloomberg.
    CORPORATE_WATCHLIST: List[Tuple] = [
        # Investment Grade
        ('AAPL', 0.0395, 100, 5.0,  None),
        ('MSFT', 0.0350, 100, 5.0,  None),
        ('JPM',  0.0450, 100, 7.0,  None),
        ('BAC',  0.0480, 100, 5.0,  None),
        ('XOM',  0.0420, 100, 10.0, None),
        # High Yield
        ('F',    0.0625, 100, 5.0,  None),
        ('T',    0.0520, 100, 7.0,  None),
        ('CCL',  0.0700, 100, 5.0,  None),
    ]

    def __init__(self, ib_host: str = IB_HOST, ib_port: int = IB_PORT):
        self.ib      = IB()
        self.host    = ib_host
        self.port    = ib_port

        self.curve    = YieldCurveBuilder()
        self.pricer   = BondPricer(self.curve)
        self.detector = MispricingDetector(self.curve, self.pricer)
        self.sizer    = DynamicPositionSizer()

        self.equity   = 0.0
        self.running  = False

    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        try:
            await self.ib.connectAsync(self.host, self.port, clientId=2)
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
        logger.warning('[EQUITY] Fallback $1M')

    # ------------------------------------------------------------------
    async def _front_month_contract(self, symbol: str):
        """Return (contract, expiry_datetime) for front-month futures."""
        try:
            raw = Future(symbol=symbol, exchange='CBOT', currency='USD')
            details = await self.ib.reqContractDetailsAsync(raw)
            if not details:
                return None
            details.sort(key=lambda d: d.contract.lastTradeDateOrContractMonth)
            front  = details[0].contract
            expiry = datetime.strptime(front.lastTradeDateOrContractMonth[:8], '%Y%m%d')
            # Roll if < 7 days to expiry
            if (expiry - datetime.now()).days < 7 and len(details) > 1:
                front  = details[1].contract
                expiry = datetime.strptime(front.lastTradeDateOrContractMonth[:8], '%Y%m%d')
            return front, expiry
        except Exception as e:
            logger.error(f'[CONTRACT] {symbol}: {e}')
            return None

    async def _live_price(self, contract) -> Optional[float]:
        """Fetch live or delayed market price from IB."""
        self.ib.reqMktData(contract, '', False, False)
        await asyncio.sleep(2)
        t = self.ib.ticker(contract)

        candidates = [t.last, t.close]
        if t.bid and t.ask and t.bid > 0:
            candidates.append((t.bid + t.ask) / 2)

        for v in candidates:
            if v and not np.isnan(v) and v > 0:
                self.ib.cancelMktData(contract)
                return float(v)

        self.ib.cancelMktData(contract)
        return None

    async def _place_order(self, contract, signal: str, sizing: Dict, price: float):
        """Place a limit order (or log in DRY_RUN mode)."""
        action = 'BUY' if 'BUY' in signal else 'SELL'
        limit  = price * (1.001 if action == 'BUY' else 0.999)

        if DRY_RUN:
            logger.info(
                f'[DRY RUN] {action} {sizing["quantity"]}x '
                f'{getattr(contract, "localSymbol", str(contract))} '
                f'@ {limit:.4f}  (${sizing["value"]:,.0f})'
            )
            return

        order = LimitOrder(action, sizing['quantity'], limit)
        self.ib.placeOrder(contract, order)
        logger.info(
            f'[ORDER] {action} {sizing["quantity"]}x '
            f'{getattr(contract, "localSymbol", "")} @ {limit:.4f}'
        )

    # ------------------------------------------------------------------
    async def scan_treasuries(self):
        """Scan T-Bill/Treasury futures — Steps 1 + 4."""
        logger.info('\n--- TREASURIES ---')
        for symbol, nom_yrs, est_coupon in self.TREASURY_SYMBOLS:
            result = await self._front_month_contract(symbol)
            if not result:
                continue
            contract, expiry = result
            years = max(0.1, (expiry - datetime.now()).days / 365.25)
            price = await self._live_price(contract)
            if not price:
                logger.warning(f'[NO PRICE] {symbol}')
                continue

            ana = self.detector.analyze_treasury(
                coupon_rate=est_coupon,
                par=100.0,
                years_to_maturity=years,
                market_price=price,
                label=getattr(contract, 'localSymbol', symbol),
            )

            logger.info(
                f'[{ana["label"]:8s}] '
                f'Mkt: {price:7.3f} | Fair: {ana["fair_price"]:7.3f} | '
                f'Alpha: {ana["alpha_bps"]:+6.1f} bps | '
                f'Dur: {ana["modified_duration"]:4.2f} | '
                f'DV01: ${ana["dv01"]:5.2f} | '
                f'Signal: {ana["signal"]} ({ana["confidence"]:.0f}%)'
            )

            if ana['signal'] != 'HOLD':
                sizing = self.sizer.calculate(self.equity, ana['confidence'], price)
                if sizing['can_trade']:
                    await self._place_order(contract, ana['signal'], sizing, price)

            await asyncio.sleep(0.5)

    # ------------------------------------------------------------------
    async def scan_corporates(self):
        """Scan corporate bond watchlist — Steps 2 + 3 + 4."""
        logger.info('\n--- CORPORATE BONDS ---')
        for ticker, coupon, par, years, ytm in self.CORPORATE_WATCHLIST:
            if ytm is None:
                ytm = self.curve.get_rate(years) / 100 + 0.015  # rf + 150 bps placeholder

            result = self.detector.analyze_corporate_bond(ticker, coupon, par, years, ytm)
            if result and result['signal'] != 'HOLD' and result['confidence'] >= MIN_CONFIDENCE:
                # Corporate bond execution requires CUSIP/ISIN lookup in production
                logger.info(
                    f'[SIGNAL] {ticker} | {result["signal"]} | '
                    f'Alpha: {result["alpha_bps"]:+.1f} bps | '
                    f'Confidence: {result["confidence"]:.0f}%'
                )
            await asyncio.sleep(1)

    # ------------------------------------------------------------------
    async def run(self):
        """Main scan loop — runs until stopped or disconnected."""
        self.running = True
        scan = 0

        logger.info('\n' + '='*80)
        logger.info('[START] Bond Arbitrage Strategy — T-Bills & Corporate Bonds')
        logger.info(f'  Scan interval : {SCAN_INTERVAL}s')
        logger.info(f'  Min alpha     : {MIN_ALPHA_BPS} bps')
        logger.info(f'  Min confidence: {MIN_CONFIDENCE}%')
        logger.info(f'  Dry run       : {DRY_RUN}')
        logger.info('='*80)

        # Initial yield curve
        self.curve.fetch_yields()

        try:
            while self.running:
                scan += 1
                logger.info(f'\n[SCAN #{scan}] {datetime.now():%Y-%m-%d %H:%M:%S}')

                await self._refresh_equity()

                # Refresh yield curve every 10 scans (~10 min)
                if scan % 10 == 1:
                    self.curve.fetch_yields()

                await self.scan_treasuries()
                await self.scan_corporates()

                logger.info(f'\n[SLEEP] Next scan in {SCAN_INTERVAL}s…')
                await asyncio.sleep(SCAN_INTERVAL)

        except asyncio.CancelledError:
            logger.info('[STOPPED]')
        except Exception as e:
            logger.error(f'[CRASH] {e}')
            import traceback
            traceback.print_exc()
        finally:
            self.running = False
            self.ib.disconnect()
            logger.info('[DISCONNECTED]')


# ==============================================================================
# ENTRY POINT
# ==============================================================================

async def main():
    bot = BondsArbitrageBot(ib_host=IB_HOST, ib_port=IB_PORT)

    if not await bot.connect():
        logger.error('[FAILED] Cannot connect to TWS')
        logger.info('Troubleshooting:')
        logger.info('  1. Is TWS open?')
        logger.info('  2. API enabled? → File → Global Config → API → Enable')
        logger.info('  3. Port: 7497 = Paper Trading | 7496 = Live Trading')
        return

    try:
        await bot.run()
    except KeyboardInterrupt:
        bot.running = False
        logger.info('[SHUTDOWN] Ctrl+C received')


if __name__ == '__main__':
    asyncio.run(main())
