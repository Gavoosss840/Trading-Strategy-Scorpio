"""
================================================================================
Modigliani-Miller Options Trading Bot - COMPLETE STANDALONE VERSION
================================================================================

Bot de trading d'options base sur le theoreme M-M ajuste
- Scanne le S&P 500 en continu via IBKR
- Calcule la valeur theorique (VL) de chaque entreprise
- Trade des OPTIONS avec strike = VL theorique (valeur cible)
- Position sizing dynamique (1-5% selon confidence)
- Runs continuously 24/7 until TWS closes

Strategie:
-----------
Si Market Cap < VL theorique (sous-evalue):
  → Achete CALL options (strike = prix cible)
  → Profit si l'action monte vers sa vraie valeur

Si Market Cap > VL theorique (surevalue):
  → Achete PUT options (strike = prix cible)
  → Profit si l'action baisse vers sa vraie valeur

MODIFICATION:
-----------
- Options à 3 mois (~90 jours)
- Pour CALL: strike le plus proche <= target price (en dessous ou égal)
- Pour PUT: strike le plus proche >= target price (au-dessus ou égal)

================================================================================
"""

# ============================================================================
# CONFIGURATION - MODIFIER ICI VOS PARAMETRES
# ============================================================================

# Connection IBKR
IB_HOST = '127.0.0.1'
IB_PORT = 7497  # 7497 = Paper Trading, 7496 = Live Trading

# Trading Parameters
DIVERGENCE_THRESHOLD = 5.0   # Minimum divergence % to trade (±5% triggers CALL/PUT signals)
SCAN_INTERVAL = 60          # Seconds between scans (60 = 1 min)
DAYS_TO_EXPIRY = 90          # Target days to expiry for options (90 = 3 months)
DRY_RUN = False              # Set to True to simulate orders without placing them

# Position Sizing
MIN_POSITION_PCT = 1.0       # Minimum position size (% of equity)
MAX_POSITION_PCT = 5.0       # Maximum position size (% of equity)
MIN_CONFIDENCE = 20.0        # Minimum confidence to trade (%)

# Risk Management
MAX_OPTIONS_UTILIZATION = 0.80  # Stop trading when 80% of options budget is used

# Logging
LOG_LEVEL = "INFO"           # DEBUG, INFO, WARNING, ERROR
LOG_FILE = "mm_options_bot.log"

# ============================================================================
# IMPORTS
# ============================================================================

import pandas as pd
import numpy as np
import yfinance as yf
from scipy.stats import norm
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from ib_insync import IB, Stock, Option, LimitOrder
import asyncio
import logging
import sys

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# MODIGLIANI-MILLER VALUATION ENGINE
# ============================================================================

class ModiglianiMillerEngine:
    """Core M-M valuation engine"""

    def __init__(self, ticker: str, risk_free_rate: float = 0.045):
        self.ticker = ticker
        self.rf = risk_free_rate
        self.stock = yf.Ticker(ticker)
        self.data = None

    def fetch_financial_data(self) -> bool:
        """Fetch financial data from yfinance"""
        try:
            info = self.stock.info
            balance_sheet = self.stock.balance_sheet
            income_stmt = self.stock.income_stmt
            cash_flow = self.stock.cashflow

            self.data = {
                'market_cap': info.get('marketCap', 0),
                'enterprise_value': info.get('enterpriseValue', 0),
                'stock_price': info.get('currentPrice', 0),
                'shares_outstanding': info.get('sharesOutstanding', 0),
                'total_debt': balance_sheet.loc['Total Debt'].iloc[0] if 'Total Debt' in balance_sheet.index else 0,
                'cash': balance_sheet.loc['Cash And Cash Equivalents'].iloc[0] if 'Cash And Cash Equivalents' in balance_sheet.index else 0,
                'total_equity': balance_sheet.loc['Stockholders Equity'].iloc[0] if 'Stockholders Equity' in balance_sheet.index else 0,
                'ebit': income_stmt.loc['EBIT'].iloc[0] if 'EBIT' in income_stmt.index else 0,
                'net_income': income_stmt.loc['Net Income'].iloc[0] if 'Net Income' in income_stmt.index else 0,
                'interest_expense': abs(income_stmt.loc['Interest Expense'].iloc[0]) if 'Interest Expense' in income_stmt.index else 0,
                'fcf': cash_flow.loc['Free Cash Flow'].iloc[0] if 'Free Cash Flow' in cash_flow.index else 0,
                'tax_rate': info.get('effectiveTaxRate', 0.25),
                'beta': info.get('beta', 1.0),
            }

            self.data['net_debt'] = self.data['total_debt'] - self.data['cash']
            self.data['debt_to_equity'] = self.data['total_debt'] / self.data['total_equity'] if self.data['total_equity'] > 0 else 0
            self.data['interest_coverage'] = self.data['ebit'] / self.data['interest_expense'] if self.data['interest_expense'] > 0 else 999

            return True

        except Exception as e:
            logger.debug(f"[SKIP] {self.ticker}: Data fetch failed - {e}")
            return False

    def calculate_tax_shield(self) -> float:
        """Calculate PV of tax shield"""
        tax_rate = self.data['tax_rate']
        interest_expense = self.data['interest_expense']

        if interest_expense == 0 or np.isnan(interest_expense):
            interest_expense = self.data['total_debt'] * 0.05

        annual_tax_shield = tax_rate * interest_expense
        discount_rate = self.rf + 0.02
        pv_tax_shield = annual_tax_shield / discount_rate if discount_rate > 0 else 0

        return pv_tax_shield

    def calculate_financial_distress_probability(self) -> float:
        """Merton model for default probability"""
        V = self.data['enterprise_value']
        D = self.data['total_debt']

        if V <= 0 or D <= 0:
            return 0

        historical_data = self.stock.history(period="1y")
        if len(historical_data) > 20:
            returns = historical_data['Close'].pct_change().dropna()
            sigma_equity = returns.std() * np.sqrt(252)
        else:
            sigma_equity = 0.30

        E = self.data['market_cap']
        sigma_assets = sigma_equity * (E / (E + D)) if (E + D) > 0 else sigma_equity

        mu = self.rf
        T = 1

        try:
            d2 = (np.log(V / D) + (mu - 0.5 * sigma_assets**2) * T) / (sigma_assets * np.sqrt(T))
            prob_default = norm.cdf(-d2)
        except:
            prob_default = 0

        return prob_default

    def calculate_financial_distress_costs(self) -> float:
        """Expected distress costs"""
        prob_default = self.calculate_financial_distress_probability()
        distress_cost_rate = 0.20
        firm_value = self.data['enterprise_value']
        pv_distress = prob_default * distress_cost_rate * firm_value
        return pv_distress

    def calculate_agency_costs(self) -> float:
        """Agency costs from leverage"""
        leverage = self.data['debt_to_equity']
        fcf = abs(self.data['fcf'])
        market_cap = self.data['market_cap']

        agency_score = 0

        if leverage > 2:
            agency_score += (leverage - 2) * 0.05

        fcf_to_value = fcf / market_cap if market_cap > 0 else 0
        if fcf_to_value > 0.10:
            agency_score += (fcf_to_value - 0.10) * 0.5

        pv_agency_costs = agency_score * market_cap
        return pv_agency_costs

    def calculate_levered_value(self) -> Dict:
        """Calculate theoretical levered value"""
        market_cap = self.data['market_cap']
        net_debt = self.data['net_debt']
        tax_shield = self.calculate_tax_shield()

        VU = market_cap + net_debt - tax_shield
        distress_costs = self.calculate_financial_distress_costs()
        agency_costs = self.calculate_agency_costs()

        VL_theoretical = VU + tax_shield - distress_costs - agency_costs

        return {
            'VU': VU,
            'tax_shield': tax_shield,
            'distress_costs': distress_costs,
            'agency_costs': agency_costs,
            'VL_theoretical': VL_theoretical
        }

    def generate_signal(self) -> Dict:
        """Generate trading signal with target price for options"""
        results = self.calculate_levered_value()
        VL_theoretical = results['VL_theoretical']
        market_cap = self.data['market_cap']
        shares_outstanding = self.data['shares_outstanding']

        # Calculate target stock price (strike for options)
        if shares_outstanding > 0:
            target_price_per_share = VL_theoretical / shares_outstanding
        else:
            target_price_per_share = 0

        current_price = self.data['stock_price']

        divergence = (VL_theoretical - market_cap) / market_cap if market_cap > 0 else 0
        divergence_pct = divergence * 100

        # Confidence based on divergence magnitude
        confidence = min(abs(divergence_pct) * 10, 100)

        # Signal generation (±5% threshold for options trading)
        if divergence_pct > 5:
            signal = "BUY_CALL"
            option_type = "CALL"
        elif divergence_pct < -5:
            signal = "BUY_PUT"
            option_type = "PUT"
        else:
            signal = "HOLD"
            option_type = None

        return {
            'signal': signal,
            'option_type': option_type,
            'confidence': confidence,
            'divergence_pct': divergence_pct,
            'VL_theoretical': VL_theoretical,
            'market_cap': market_cap,
            'current_price': current_price,
            'target_price': target_price_per_share,
            'shares_outstanding': shares_outstanding,
            **results
        }


# ============================================================================
# POSITION SIZING
# ============================================================================

class DynamicPositionSizer:
    """Position sizing based on confidence (for options) with portfolio allocation"""

    def __init__(self, min_pct: float = MIN_POSITION_PCT, max_pct: float = MAX_POSITION_PCT, min_conf: float = MIN_CONFIDENCE):
        self.min_position_pct = min_pct
        self.max_position_pct = max_pct
        self.min_confidence = min_conf
        self.options_allocation = 0.25  # 1/4 of total portfolio allocated to options

    def calculate_position_size(self, total_equity: float, confidence: float, option_price: float) -> Dict:
        """
        Calculate number of option contracts based on confidence
        - Options budget = 1/4 of total portfolio
        - Position size per stock = 1-5% of options budget (based on confidence)
        """

        if confidence < self.min_confidence:
            return {
                'can_trade': False,
                'reason': f'Low confidence ({confidence:.1f}%)',
                'quantity': 0,
                'value': 0,
                'actual_pct_of_options_budget': 0,
                'actual_pct_of_total_portfolio': 0
            }

        if total_equity <= 0:
            return {
                'can_trade': False,
                'reason': 'No equity',
                'quantity': 0,
                'value': 0,
                'actual_pct_of_options_budget': 0,
                'actual_pct_of_total_portfolio': 0
            }

        # Calculate options budget (1/4 of total portfolio)
        options_budget = total_equity * self.options_allocation

        # Linear interpolation for position size based on confidence
        # confidence: 20% -> 1% of options budget
        # confidence: 100% -> 5% of options budget
        normalized = (confidence - self.min_confidence) / (100 - self.min_confidence)
        normalized = max(0, min(1, normalized))

        position_pct = self.min_position_pct + (self.max_position_pct - self.min_position_pct) * normalized
        position_value = options_budget * (position_pct / 100)

        # Options: 1 contract = 100 shares
        if option_price > 0:
            contracts = int(position_value / (option_price * 100))
        else:
            contracts = 0

        if contracts == 0:
            return {
                'can_trade': False,
                'reason': f'Insufficient capital for {option_price:.2f}/share option',
                'quantity': 0,
                'value': 0,
                'actual_pct_of_options_budget': 0,
                'actual_pct_of_total_portfolio': 0
            }

        actual_value = contracts * option_price * 100
        actual_pct_of_options_budget = (actual_value / options_budget) * 100
        actual_pct_of_total_portfolio = (actual_value / total_equity) * 100

        return {
            'can_trade': True,
            'quantity': contracts,
            'target_pct': position_pct,
            'actual_pct_of_options_budget': actual_pct_of_options_budget,
            'actual_pct_of_total_portfolio': actual_pct_of_total_portfolio,
            'value': actual_value,
            'options_budget': options_budget,
            'total_equity': total_equity,
            'reason': f'{confidence:.1f}% conf -> {position_pct:.1f}% of options budget'
        }


# ============================================================================
# MAIN BOT
# ============================================================================

class MMOptionsBot:
    """Continuous M-M Options Trading Bot"""

    def __init__(self,
                 ib_host: str = IB_HOST,
                 ib_port: int = IB_PORT,
                 divergence_threshold: float = DIVERGENCE_THRESHOLD,
                 scan_interval: int = SCAN_INTERVAL,
                 days_to_expiry: int = DAYS_TO_EXPIRY):

        self.ib = IB()
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.divergence_threshold = divergence_threshold
        self.scan_interval = scan_interval
        self.days_to_expiry = days_to_expiry

        self.position_sizer = DynamicPositionSizer()
        self.account_equity = 0.0
        self.account_number = None
        self.running = False
        self.tickers_to_scan = []
        self.open_positions = set()  # Track open option positions to avoid duplicates

    async def get_sp500_tickers(self) -> List[str]:
        """Get full S&P 500 tickers from Wikipedia"""
        try:
            logger.info("[S&P 500] Fetching from Wikipedia...")
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            # Add User-Agent header to avoid 403 errors
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            import urllib.request
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                html_content = response.read()
            table = pd.read_html(html_content)[0]
            sp500_tickers = table['Symbol'].str.replace('.', '-').tolist()
            logger.info(f"[S&P 500] Retrieved {len(sp500_tickers)} tickers")
            return sp500_tickers
        except Exception as e:
            logger.warning(f"[WARN] S&P 500 fetch failed: {e}")
            return []

    async def get_nasdaq100_tickers(self) -> List[str]:
        """Get full Nasdaq 100 tickers from Wikipedia"""
        try:
            logger.info("[NASDAQ 100] Fetching from Wikipedia...")
            url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            # Add User-Agent header to avoid 403 errors
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            import urllib.request
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                html_content = response.read()
            tables = pd.read_html(html_content)
            # The constituents table is usually the 4th table
            for table in tables:
                if 'Ticker' in table.columns or 'Symbol' in table.columns:
                    ticker_col = 'Ticker' if 'Ticker' in table.columns else 'Symbol'
                    nasdaq100_tickers = table[ticker_col].str.replace('.', '-').tolist()
                    logger.info(f"[NASDAQ 100] Retrieved {len(nasdaq100_tickers)} tickers")
                    return nasdaq100_tickers
            logger.warning("[WARN] Could not find Nasdaq 100 table")
            return []
        except Exception as e:
            logger.warning(f"[WARN] Nasdaq 100 fetch failed: {e}")
            return []

    async def get_all_tickers(self) -> List[str]:
        """Get S&P 500 + Nasdaq 100 tickers (combined, deduplicated)"""
        sp500 = await self.get_sp500_tickers()
        nasdaq100 = await self.get_nasdaq100_tickers()

        # Combine and remove duplicates
        all_tickers = list(set(sp500 + nasdaq100))

        logger.info(f"[COMBINED] {len(sp500)} S&P 500 + {len(nasdaq100)} Nasdaq 100")
        logger.info(f"[TOTAL] {len(all_tickers)} unique tickers (after deduplication)")

        # Fallback if both failed
        if len(all_tickers) == 0:
            logger.warning("[FALLBACK] Using curated list")
            return [
                "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
                "JPM", "BAC", "GS", "WFC", "C", "MS", "BLK",
                "XOM", "CVX", "COP", "SLB",
                "JNJ", "PFE", "UNH", "ABBV", "LLY",
                "WMT", "COST", "TGT", "HD", "LOW",
                "KO", "PEP", "MCD", "SBUX",
                "DIS", "NFLX", "CMCSA"
            ]

        return sorted(all_tickers)

    async def validate_tickers_with_ibkr(self, tickers: List[str]) -> List[str]:
        """Validate tickers are tradeable"""
        valid = []
        for ticker in tickers[:20]:
            try:
                contract = Stock(ticker, 'SMART', 'USD')
                qualified = await self.ib.qualifyContractsAsync(contract)
                if qualified:
                    valid.append(ticker)
            except:
                pass
            await asyncio.sleep(0.1)
        return valid

    async def connect(self) -> bool:
        """Connect to TWS and fetch S&P 500 + Nasdaq 100"""
        try:
            await self.ib.connectAsync(self.ib_host, self.ib_port, clientId=10)
            logger.info(f"[CONNECTED] IB TWS at {self.ib_host}:{self.ib_port}")

            await self.update_account_info()

            logger.info("\n[FETCHING TICKERS] Retrieving S&P 500 + Nasdaq 100...")
            self.tickers_to_scan = await self.get_all_tickers()

            if not self.tickers_to_scan:
                logger.warning("[WARNING] No tickers loaded")
                return False

            logger.info(f"[READY] {len(self.tickers_to_scan)} stocks loaded for scanning\n")
            return True

        except Exception as e:
            logger.error(f"[ERROR] Connection failed: {e}")
            return False

    async def update_account_info(self):
        """Get account equity and sync open positions"""
        try:
            accounts = self.ib.managedAccounts()
            if accounts:
                self.account_number = accounts[0]

            account_values = self.ib.accountValues()
            for av in account_values:
                if av.tag == 'NetLiquidation':
                    if av.currency == 'USD':
                        self.account_equity = float(av.value)
                        logger.info(f"[EQUITY] ${self.account_equity:,.2f} USD")
                        break
                    elif av.currency == 'EUR':
                        self.account_equity = float(av.value) * 1.08
                        logger.info(f"[EQUITY] EUR{float(av.value):,.2f} ~= ${self.account_equity:,.2f} USD")
                        break

            if self.account_equity == 0:
                logger.warning("[WARNING] Equity = $0, using $100k default")
                self.account_equity = 100000.0

            # Sync open option positions from TWS
            positions = self.ib.positions()
            option_count = 0
            total_options_value = 0.0

            for position in positions:
                if position.contract.secType == 'OPT' and position.position != 0:
                    ticker = position.contract.symbol
                    option_type = 'CALL' if position.contract.right == 'C' else 'PUT'
                    position_key = f"{ticker}_{option_type}"
                    self.open_positions.add(position_key)
                    option_count += 1

                    # Calculate market value of options position
                    market_value = abs(position.position) * position.avgCost * 100
                    total_options_value += market_value

            if option_count > 0:
                options_budget = self.account_equity * 0.25
                utilization_pct = (total_options_value / options_budget * 100) if options_budget > 0 else 0

                logger.info(f"[POSITIONS] Synced {option_count} open option positions from TWS")
                logger.info(f"[POSITIONS] Tracked: {', '.join(sorted(self.open_positions))}")
                logger.info(f"[OPTIONS BUDGET] ${total_options_value:,.2f} / ${options_budget:,.2f} ({utilization_pct:.1f}% used)")

        except Exception as e:
            logger.error(f"[ERROR] Account update failed: {e}")

    def get_options_budget_utilization(self) -> float:
        """Calculate current options budget utilization percentage"""
        try:
            positions = self.ib.positions()
            total_options_value = 0.0

            for position in positions:
                if position.contract.secType == 'OPT' and position.position != 0:
                    market_value = abs(position.position) * position.avgCost * 100
                    total_options_value += market_value

            options_budget = self.account_equity * 0.25
            utilization = (total_options_value / options_budget) if options_budget > 0 else 0
            return utilization

        except Exception as e:
            logger.debug(f"[ERROR] Failed to calculate budget utilization: {e}")
            return 0.0

    async def find_option_chain(self, ticker: str, target_strike: float, option_type: str) -> Optional[Option]:
        """
        Find option contract close to target strike on 3-month options.
        For CALL: closest strike below or equal to target
        For PUT: closest strike above or equal to target
        """
        try:
            stock = Stock(ticker, 'SMART', 'USD')
            qualified_stock = await self.ib.qualifyContractsAsync(stock)

            if not qualified_stock:
                logger.debug(f"[SKIP] {ticker} - Could not qualify stock")
                return None

            chains = await self.ib.reqSecDefOptParamsAsync(
                stock.symbol, '', stock.secType, stock.conId
            )

            # Silently skip if no option chains available
            if not chains:
                logger.debug(f"[SKIP] {ticker} - No option chains available")
                return None

            # Find expiry closest to 3 months (~90 days)
            target_date = datetime.now() + timedelta(days=90)

            best_chain = None
            min_date_diff = float('inf')
            best_expiry = None

            for chain in chains:
                for exp_str in chain.expirations:
                    exp_date = datetime.strptime(exp_str, '%Y%m%d')
                    date_diff = abs((exp_date - target_date).days)

                    if date_diff < min_date_diff:
                        min_date_diff = date_diff
                        best_chain = chain
                        best_expiry = exp_str

            # Silently skip if no suitable expiry found
            if not best_chain:
                logger.debug(f"[SKIP] {ticker} - No suitable expiry found")
                return None

            # Choose strike based on target price and option type
            strikes = sorted(best_chain.strikes)

            if option_type == "CALL":
                # For CALL: find closest strike <= target_strike
                eligible_strikes = [s for s in strikes if s <= target_strike]
                if eligible_strikes:
                    closest_strike = max(eligible_strikes)  # Closest from below
                else:
                    # If no strike below target, take the lowest available
                    closest_strike = min(strikes)
                    logger.info(f"[NOTE] {ticker} CALL - No strike <= target ${target_strike:.2f}, using lowest: ${closest_strike:.2f}")
            else:  # PUT
                # For PUT: find closest strike >= target_strike
                eligible_strikes = [s for s in strikes if s >= target_strike]
                if eligible_strikes:
                    closest_strike = min(eligible_strikes)  # Closest from above
                else:
                    # If no strike above target, take the highest available
                    closest_strike = max(strikes)
                    logger.info(f"[NOTE] {ticker} PUT - No strike >= target ${target_strike:.2f}, using highest: ${closest_strike:.2f}")

            option = Option(
                ticker,
                best_expiry,
                closest_strike,
                option_type[0],
                'SMART'
            )

            qualified = await self.ib.qualifyContractsAsync(option)

            if qualified:
                logger.info(f"[OPTION FOUND] {ticker} {option_type} strike ${closest_strike:.2f} exp:{best_expiry}")
                logger.info(f"  M-M Target: ${target_strike:.2f} | Selected strike: ${closest_strike:.2f}")
                return qualified[0]
            else:
                # Silently skip if option could not be qualified
                logger.debug(f"[SKIP] {ticker} - Could not qualify option")
                return None

        except Exception as e:
            # Only log errors in debug mode, silently skip otherwise
            logger.debug(f"[SKIP] {ticker} - Option chain error: {e}")
            return None

    async def get_option_price(self, option: Option) -> Optional[float]:
        """Get current option price"""
        try:
            self.ib.reqMktData(option, '', False, False)
            await asyncio.sleep(2)

            ticker = self.ib.ticker(option)

            if ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
                price = (ticker.bid + ticker.ask) / 2
                logger.info(f"  Option Price: ${price:.2f} (bid: ${ticker.bid:.2f}, ask: ${ticker.ask:.2f})")
                self.ib.cancelMktData(option)
                return price

            elif ticker.last and ticker.last > 0:
                price = ticker.last
                logger.info(f"  Option Price: ${price:.2f} (last)")
                self.ib.cancelMktData(option)
                return price

            else:
                logger.warning(f"  No option price available")
                self.ib.cancelMktData(option)
                return None

        except Exception as e:
            logger.error(f"[ERROR] Option price failed: {e}")
            return None

    async def analyze_ticker(self, ticker: str) -> Optional[Dict]:
        """Analyze ticker for option opportunity"""
        try:
            engine = ModiglianiMillerEngine(ticker, risk_free_rate=0.045)

            if not engine.fetch_financial_data():
                return None

            signal = engine.generate_signal()

            # Skip if target price (strike) is invalid
            if signal['target_price'] <= 0:
                logger.debug(f"[SKIP] {ticker}: Invalid target price ${signal['target_price']:.2f}")
                return None

            if abs(signal['divergence_pct']) >= self.divergence_threshold:
                return {
                    'ticker': ticker,
                    **signal
                }

            return None

        except Exception as e:
            logger.debug(f"[SKIP] {ticker}: {e}")
            return None

    async def scan_opportunities(self) -> List[Dict]:
        """Scan for option opportunities in parallel batches"""
        total_tickers = len(self.tickers_to_scan)
        logger.info(f"\n[SCANNING] {total_tickers} tickers in parallel...")
        logger.info(f"[PROGRESS] This will take approximately {total_tickers * 0.5 / 60:.1f} minutes...")

        opportunities = []
        batch_size = 50  # Process 50 tickers at a time

        for i in range(0, total_tickers, batch_size):
            batch = self.tickers_to_scan[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_tickers + batch_size - 1) // batch_size

            logger.info(f"[BATCH {batch_num}/{total_batches}] Scanning {len(batch)} tickers ({i+1}-{min(i+len(batch), total_tickers)})...")

            # Scan batch in parallel
            tasks = [self.analyze_ticker(ticker) for ticker in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for ticker, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.debug(f"[ERROR] {ticker}: {result}")
                elif result:
                    opportunities.append(result)
                    logger.info(f"  [OPPORTUNITY] {ticker}: {result['signal']} ({result['divergence_pct']:+.1f}%, target: ${result['target_price']:.2f})")

            # Small delay between batches to avoid overloading APIs
            await asyncio.sleep(0.5)

        opportunities.sort(key=lambda x: abs(x['divergence_pct']), reverse=True)
        logger.info(f"\n[SCAN COMPLETE] Found {len(opportunities)} opportunities out of {total_tickers} tickers")

        return opportunities

    async def execute_option_trade(self, opportunity: Dict):
        """Execute option trade. Silently skips if no options available or position already open."""
        ticker = opportunity['ticker']
        signal = opportunity['signal']
        option_type = opportunity['option_type']
        target_strike = opportunity['target_price']
        confidence = opportunity['confidence']
        divergence = opportunity['divergence_pct']
        current_price = opportunity['current_price']

        # Create position key to track duplicates
        position_key = f"{ticker}_{option_type}"

        # Skip if we already have a position for this ticker+type
        if position_key in self.open_positions:
            logger.info(f"[SKIP] {ticker} - Already have open {option_type} position")
            return

        # Check options budget utilization - stop if > 80%
        budget_utilization = self.get_options_budget_utilization()
        if budget_utilization >= MAX_OPTIONS_UTILIZATION:
            logger.warning(f"[BUDGET LIMIT] Options budget {budget_utilization*100:.1f}% used (max {MAX_OPTIONS_UTILIZATION*100:.0f}%) - Skipping {ticker}")
            return

        logger.info(f"\n{'='*80}")
        logger.info(f"[ANALYZING OPTIONS] {ticker}")
        logger.info(f"{'='*80}")
        logger.info(f"Current: ${current_price:.2f} | Target: ${target_strike:.2f} | Signal: {signal}")
        logger.info(f"Divergence: {divergence:+.1f}%")

        option_contract = await self.find_option_chain(ticker, target_strike, option_type)

        # Silently skip if no option chain available
        if not option_contract:
            logger.debug(f"[SKIP] {ticker} - No tradeable options available")
            return

        option_price = await self.get_option_price(option_contract)

        # Silently skip if no valid option price
        if not option_price or option_price <= 0:
            logger.debug(f"[SKIP] {ticker} - Invalid option price")
            return

        sizing = self.position_sizer.calculate_position_size(
            self.account_equity,
            confidence,
            option_price
        )

        if not sizing['can_trade']:
            logger.info(f"[NO TRADE] {ticker}: {sizing['reason']}")
            return

        # Log detailed position sizing
        logger.info(f"\n{'='*80}")
        logger.info(f"[POSITION SIZING] {ticker}")
        logger.info(f"{'='*80}")
        logger.info(f"Total Portfolio Value: ${sizing['total_equity']:,.2f}")
        logger.info(f"Options Budget (25%): ${sizing['options_budget']:,.2f}")
        logger.info(f"Confidence: {confidence:.1f}% -> Target: {sizing['target_pct']:.1f}% of options budget")
        logger.info(f"Position: {sizing['quantity']} contracts @ ${option_price:.2f}/share")
        logger.info(f"Position Value: ${sizing['value']:,.2f}")
        logger.info(f"  - {sizing['actual_pct_of_options_budget']:.2f}% of options budget")
        logger.info(f"  - {sizing['actual_pct_of_total_portfolio']:.2f}% of total portfolio")
        logger.info(f"{'='*80}")

        try:
            action = 'BUY'
            quantity = sizing['quantity']
            limit_price = option_price * 1.02

            if DRY_RUN:
                logger.info(f"\n{'='*80}")
                logger.info(f"[DRY RUN - ORDER SIMULATED] {action} {quantity} {ticker} {option_type} OPTIONS")
                logger.info(f"{'='*80}")
                logger.info(f"Strike: ${option_contract.strike:.2f} | Expiry: {option_contract.lastTradeDateOrContractMonth}")
                logger.info(f"Price: ${limit_price:.2f}/share | Total: ${sizing['value']:,.2f}")
                logger.info(f"Position: {sizing['actual_pct_of_options_budget']:.2f}% of options budget | {sizing['actual_pct_of_total_portfolio']:.2f}% of total portfolio")
                logger.info(f"Reason: {divergence:+.1f}% mispricing | {confidence:.1f}% confidence")
                logger.info(f"[NOTE] Set DRY_RUN = False to place real orders")
                logger.info(f"{'='*80}\n")
            else:
                order = LimitOrder(action, quantity, limit_price)
                trade = self.ib.placeOrder(option_contract, order)

                logger.info(f"\n{'='*80}")
                logger.info(f"[ORDER PLACED] {action} {quantity} {ticker} {option_type} OPTIONS")
                logger.info(f"{'='*80}")
                logger.info(f"Strike: ${option_contract.strike:.2f} | Expiry: {option_contract.lastTradeDateOrContractMonth}")
                logger.info(f"Price: ${limit_price:.2f}/share | Total: ${sizing['value']:,.2f}")
                logger.info(f"Position: {sizing['actual_pct_of_options_budget']:.2f}% of options budget | {sizing['actual_pct_of_total_portfolio']:.2f}% of total portfolio")
                logger.info(f"Reason: {divergence:+.1f}% mispricing | {confidence:.1f}% confidence")
                logger.info(f"Order ID: {trade.order.orderId}")
                logger.info(f"Status: {trade.orderStatus.status}")
                logger.info(f"{'='*80}\n")

                # Add to open positions tracker
                self.open_positions.add(position_key)
                logger.info(f"[TRACKED] Added {position_key} to open positions")

                # Wait and check order status
                await asyncio.sleep(3)
                logger.info(f"[ORDER STATUS] {ticker}: {trade.orderStatus.status}")

        except Exception as e:
            logger.error(f"[ERROR] Order failed for {ticker}: {e}")
            import traceback
            traceback.print_exc()

    async def run_continuous(self):
        """Main trading loop"""
        self.running = True

        logger.info("\n" + "="*80)
        logger.info("[BOT START] Modigliani-Miller OPTIONS Trading Bot")
        logger.info("="*80)
        logger.info(f"Scan Interval: {self.scan_interval}s ({self.scan_interval/60:.1f} min)")
        logger.info(f"Divergence Threshold: {self.divergence_threshold}%")
        logger.info(f"Position Range: {MIN_POSITION_PCT}-{MAX_POSITION_PCT}% of equity")
        logger.info(f"Option Expiry: {self.days_to_expiry} days (~3 months)")
        logger.info("="*80)

        scan_count = 0

        try:
            while self.running:
                scan_count += 1
                logger.info(f"\n[SCAN #{scan_count}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                await self.update_account_info()

                opportunities = await self.scan_opportunities()

                # Execute trades for all opportunities found (sorted by divergence)
                if opportunities:
                    budget_util = self.get_options_budget_utilization()
                    logger.info(f"\n[TRADING] Processing {len(opportunities)} opportunities...")
                    logger.info(f"[BUDGET] Current options utilization: {budget_util*100:.1f}% (max {MAX_OPTIONS_UTILIZATION*100:.0f}%)")

                    trades_executed = 0
                    for opp in opportunities:
                        # Check budget before each trade
                        current_util = self.get_options_budget_utilization()
                        if current_util >= MAX_OPTIONS_UTILIZATION:
                            logger.warning(f"[BUDGET LIMIT REACHED] Stopping trading - {current_util*100:.1f}% of options budget used")
                            logger.info(f"[SUMMARY] Executed {trades_executed} trades, skipped {len(opportunities) - trades_executed} due to budget limit")
                            break

                        await self.execute_option_trade(opp)
                        trades_executed += 1
                        await asyncio.sleep(2)  # Small delay between trades

                    if trades_executed == len(opportunities):
                        logger.info(f"[SUMMARY] Executed all {trades_executed} trades")
                else:
                    logger.info("[NO OPPORTUNITIES] No trading signals found this scan")

                logger.info(f"\n[SLEEP] Next scan in {self.scan_interval}s ({self.scan_interval/60:.1f} min)...")
                await asyncio.sleep(self.scan_interval)

        except asyncio.CancelledError:
            logger.info("\n[STOPPED] Bot stopped by user")
        except Exception as e:
            logger.error(f"\n[ERROR] Bot crashed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.running = False
            self.ib.disconnect()
            logger.info("[DISCONNECTED] Bot shutdown complete")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

async def main():
    """Main entry point"""

    logger.info("\n" + "="*80)
    logger.info("MM OPTIONS BOT - CONFIGURATION")
    logger.info("="*80)
    logger.info(f"IB Host: {IB_HOST}:{IB_PORT}")
    logger.info(f"Divergence Threshold: {DIVERGENCE_THRESHOLD}%")
    logger.info(f"Scan Interval: {SCAN_INTERVAL}s")
    logger.info(f"Days to Expiry: {DAYS_TO_EXPIRY} (~3 months)")
    logger.info(f"Position Size: {MIN_POSITION_PCT}-{MAX_POSITION_PCT}%")
    logger.info("="*80)

    bot = MMOptionsBot(
        ib_host=IB_HOST,
        ib_port=IB_PORT,
        divergence_threshold=DIVERGENCE_THRESHOLD,
        scan_interval=SCAN_INTERVAL,
        days_to_expiry=DAYS_TO_EXPIRY
    )

    if not await bot.connect():
        logger.error("[FAILED] Could not connect to TWS")
        logger.info("\nTroubleshooting:")
        logger.info("1. Is TWS open?")
        logger.info("2. Is API enabled? (File -> Global Configuration -> API)")
        logger.info("3. Port correct? (7497=Paper, 7496=Live)")
        logger.info("4. Options trading enabled?")
        return

    try:
        await bot.run_continuous()
    except KeyboardInterrupt:
        logger.info("\n[CTRL+C] Shutting down...")
        bot.running = False


if __name__ == "__main__":
    asyncio.run(main())