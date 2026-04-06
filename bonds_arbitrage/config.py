"""
================================================================================
Configuration — Government Bond Arbitrage  (Scorpio v2.0)
================================================================================
Modifier ces paramètres pour ajuster la stratégie.
"""

# ==============================================================================
# CONNEXION IBKR
# ==============================================================================
IB_HOST = '127.0.0.1'
IB_PORT = 7497          # 7497 = Paper Trading | 7496 = Live Trading

# ==============================================================================
# PARAMÈTRES Z-SCORE (spread inter-pays)
# ==============================================================================
ZSCORE_WINDOW  = 252    # fenêtre pour la MOYENNE (niveau de long terme)
ZSCORE_STD_WIN = 60     # fenêtre pour la VOLATILITÉ (contexte récent)
ZSCORE_ENTRY   = 1.5    # entrée  quand |z| > 1.5
ZSCORE_EXIT    = 0.3    # sortie  quand |z| < 0.3
ZSCORE_STOP    = 3.0    # stop-loss si |z| > 3.0

# ==============================================================================
# PARAMÈTRES NPV (confirmation secondaire)
# ==============================================================================
NPV_THRESHOLD  = 0.05   # alpha NPV minimum (%) pour que la confirmation soit active

# ==============================================================================
# SIZING
# ==============================================================================
MIN_CONFIDENCE   = 30.0  # score minimum (0-100) pour trader
MIN_POSITION_PCT = 1.0   # taille min de position (% du capital)
MAX_POSITION_PCT = 5.0   # taille max de position (% du capital)

# ==============================================================================
# TAKE PROFIT / STOP LOSS
# ==============================================================================
TP_ZSCORE_TARGET      = 0.3   # TP quand z revient à ±0.3 (≈80% du mouvement récupéré)
SL_ZSCORE_EXTEND      = 1.0   # SL si z s'étend de +1.0 au-delà du z d'entrée
TP_SL_UPDATE_THRESHOLD= 0.15  # recalculer TP/SL si le prix a bougé de >0.15%

# ==============================================================================
# TIMING
# ==============================================================================
REBALANCE_HOUR   = 14   # heure UTC du rebalancing quotidien (14h = 10h ET / 15h Paris)
MONITOR_INTERVAL = 300  # secondes entre checks TP/SL (5 min)
DATA_REFRESH_H   = 6    # heures entre refresh des données souveraines

# ==============================================================================
# BACKTEST
# ==============================================================================
BT_DEFAULT_FROM    = '2019-01-01'
BT_DEFAULT_CAPITAL = 1_000_000.0

# ==============================================================================
# DIVERS
# ==============================================================================
DRY_RUN        = True           # True = log uniquement | False = ordres réels
PORTFOLIO_FILE = 'portfolio.json'
LOG_FILE       = 'bonds_arbitrage.log'

# ==============================================================================
# PAIRES SURVEILLÉES  (pays_A, pays_B, maturité_années)
# ==============================================================================
SPREAD_PAIRS = [
    ('US', 'DE', 10),   # US 10Y vs Bund       — référence mondiale
    ('US', 'DE',  2),   # US 2Y  vs Schatz     — front de courbe Fed/BCE
    ('US', 'DE',  5),   # US 5Y  vs Bobl
    ('US', 'UK', 10),   # US 10Y vs Gilt        — Fed vs BoE
    ('US', 'JP', 10),   # US 10Y vs JGB         — Fed vs BoJ (YCC)
    ('US', 'FR', 10),   # US 10Y vs OAT         — risque politique EU
    ('DE', 'IT', 10),   # Bund vs BTP           — spread périphérique EU
    ('DE', 'FR', 10),   # Bund vs OAT           — core EU
]
