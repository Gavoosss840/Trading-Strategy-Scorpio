"""
Data layer — Sovereign Yield Fetcher
Sources : FRED (US daily), ECB SDW (EU daily), BoE, FRED mensuel fallback
"""

import pandas as pd
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from .config import ZSCORE_WINDOW

logger = logging.getLogger(__name__)


class SovereignYieldFetcher:
    """
    Fetch sovereign bond yields for US, DE, UK, JP, FR, IT.

    Sources (par ordre de priorité) :
    • US       → FRED daily  (DGS series)
    • DE/FR/IT → ECB SDW API daily  →  fallback FRED mensuel
    • UK       → BoE API  →  fallback FRED mensuel
    • JP       → FRED mensuel
    """

    FRED_US: Dict[int, str] = {
        1: 'DGS1', 2: 'DGS2', 5: 'DGS5', 10: 'DGS10', 30: 'DGS30',
    }
    FRED_INTL: Dict[str, Dict[int, str]] = {
        'DE': {2: 'IRLTST01DEM156N', 10: 'IRLTLT01DEM156N'},
        'FR': {10: 'IRLTLT01FRM156N'},
        'IT': {10: 'IRLTLT01ITM156N'},
        'UK': {10: 'IRLTLT01GBM156N'},
        'JP': {10: 'IRLTLT01JPM156N'},
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

    # ── Fetchers internes ─────────────────────────────────────────────

    def _fred(self, sid: str) -> Optional[pd.Series]:
        start = (datetime.now() - timedelta(days=self.history_days)).strftime('%Y-%m-%d')
        try:
            r = requests.get(
                f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd={start}',
                timeout=8,
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
                timeout=10,
            )
            if r.status_code != 200:
                return None
            lines = r.text.strip().split('\n')
            hdr   = lines[0].split(',')
            di = next((i for i, h in enumerate(hdr) if 'TIME_PERIOD' in h), None)
            vi = next((i for i, h in enumerate(hdr) if 'OBS_VALUE'   in h), None)
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

    # ── API publique ──────────────────────────────────────────────────

    def fetch_all(self) -> bool:
        """Fetch toutes les séries et mettre en cache."""
        logger.info('[YIELDS] Fetching sovereign yields...')
        cache: Dict[str, Dict[int, pd.Series]] = {}

        # USA — FRED daily
        cache['US'] = {}
        for mat, sid in self.FRED_US.items():
            s = self._fred(sid)
            if s is not None and len(s) > 20:
                cache['US'][mat] = s
                logger.info(f'  US {mat:2d}Y : {len(s):4d} pts  last={s.iloc[-1]:.3f}%')

        # Europe — ECB daily + fallback FRED mensuel
        for country in ('DE', 'FR', 'IT'):
            cache[country] = {}
            for mat, ecb_key in self.ECB_SERIES.get(country, {}).items():
                s = self._ecb(ecb_key)
                if s is not None and len(s) > 10:
                    cache[country][mat] = s
                    logger.info(f'  {country} {mat:2d}Y (ECB) : {len(s):4d} pts  last={s.iloc[-1]:.3f}%')
                else:
                    fk = self.FRED_INTL.get(country, {}).get(mat)
                    if fk:
                        s = self._fred(fk)
                        if s is not None and len(s) > 5:
                            cache[country][mat] = s
                            logger.info(f'  {country} {mat:2d}Y (FRED): {len(s):4d} pts  last={s.iloc[-1]:.3f}%')
            for mat, fk in self.FRED_INTL.get(country, {}).items():
                if mat not in cache[country]:
                    s = self._fred(fk)
                    if s is not None and len(s) > 5:
                        cache[country][mat] = s

        # UK
        cache['UK'] = {}
        s = self._fred('IRLTLT01GBM156N')
        if s is not None and len(s) > 5:
            cache['UK'][10] = s
            logger.info(f'  UK 10Y (FRED): {len(s):4d} pts  last={s.iloc[-1]:.3f}%')

        # Japon
        cache['JP'] = {}
        s = self._fred('IRLTLT01JPM156N')
        if s is not None and len(s) > 5:
            cache['JP'][10] = s
            logger.info(f'  JP 10Y (FRED): {len(s):4d} pts  last={s.iloc[-1]:.3f}%')

        self._cache      = cache
        self.last_update = datetime.now()
        return bool(cache.get('US'))

    def latest(self, country: str, maturity: int) -> Optional[float]:
        """Dernier taux disponible (%)."""
        s = self._cache.get(country, {}).get(maturity)
        return float(s.iloc[-1]) if s is not None and len(s) > 0 else None

    def history(self, country: str, maturity: int) -> Optional[pd.Series]:
        """Série historique complète."""
        return self._cache.get(country, {}).get(maturity)

    def history_until(self, country: str, maturity: int,
                      until: pd.Timestamp) -> Optional[pd.Series]:
        """Série tronquée à une date (pour backtesting walk-forward)."""
        s = self.history(country, maturity)
        return s[s.index <= until] if s is not None else None
