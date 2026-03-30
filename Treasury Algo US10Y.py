"""
Interactive Brokers - US Treasury Arbitrage Bot
- Automatic contract rollover (always trades front month)
- Auto-fetch bond parameters on contract change
- Runs continuously until TWS is closed
- Dynamic position sizing (1-5% based on confidence)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from ib_insync import IB, Future, LimitOrder
import asyncio
import logging
import sys
import requests
from bs4 import BeautifulSoup

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('treasury_arbitrage.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BondParameterFetcher:
    """
    Fetch live bond parameters from US Treasury official sources
    """
    
    @staticmethod
    def fetch_treasury_yields() -> Dict[str, float]:
        """
        Fetch current US Treasury yields from treasury.gov
        Returns yields for 2Y, 5Y, 10Y, 30Y
        """
        try:
            # Try FRED API (Federal Reserve Economic Data) - Free and reliable
            fred_urls = {
                '2Y': 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2&cosd=2024-01-01',
                '5Y': 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS5&cosd=2024-01-01',
                '10Y': 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10&cosd=2024-01-01',
                '30Y': 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS30&cosd=2024-01-01'
            }
            
            yields = {}
            for maturity, url in fred_urls.items():
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    lines = response.text.strip().split('\n')
                    # Get last line (most recent data)
                    last_line = lines[-1]
                    parts = last_line.split(',')
                    if len(parts) >= 2 and parts[1] != '.':
                        yields[maturity] = float(parts[1])
            
            if len(yields) == 4:
                logger.info(f"[YIELDS FETCHED] 2Y: {yields['2Y']}%, 5Y: {yields['5Y']}%, 10Y: {yields['10Y']}%, 30Y: {yields['30Y']}%")
                return yields
            
        except Exception as e:
            logger.warning(f"[WARNING] Could not fetch FRED yields: {e}")
        
        # Fallback to default yields
        logger.info("[FALLBACK] Using default treasury yields")
        return {
            '2Y': 4.50,
            '5Y': 4.20,
            '10Y': 4.35,
            '30Y': 4.55
        }
    
    @staticmethod
    def fetch_current_treasury_auction_data(maturity_years: int) -> Dict:
        """
        Fetch actual auction data for US Treasuries from TreasuryDirect
        Gets real coupon rates and par values from recent auctions
        
        Args:
            maturity_years: 2, 5, 10, or 30
            
        Returns:
            Dict with coupon_rate and par_value
        """
        try:
            # TreasuryDirect auction data API
            # This gets recent auction results with actual coupon rates
            base_url = "https://www.treasurydirect.gov/TA_WS/securities/search"
            
            # Map maturity to security type
            security_type_map = {
                2: "Note",
                5: "Note", 
                10: "Note",
                30: "Bond"
            }
            
            security_type = security_type_map.get(maturity_years, "Note")
            
            params = {
                "format": "json",
                "securityType": security_type,
                "maturityYears": str(maturity_years),
                "pagenum": "1",
                "pagesize": "1"  # Get most recent auction
            }
            
            response = requests.get(base_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data and len(data) > 0:
                    auction = data[0]
                    
                    # Extract coupon rate (InterestRate field)
                    coupon_rate = float(auction.get('interestRate', 0)) / 100
                    
                    # Par value is always $100 for US Treasuries
                    par_value = 100.0
                    
                    issue_date = auction.get('issueDate', 'Unknown')
                    maturity_date = auction.get('maturityDate', 'Unknown')
                    
                    logger.info(f"[AUCTION DATA] {maturity_years}Y Treasury:")
                    logger.info(f"  Coupon: {coupon_rate*100:.3f}%")
                    logger.info(f"  Par Value: ${par_value}")
                    logger.info(f"  Issue Date: {issue_date}")
                    logger.info(f"  Maturity: {maturity_date}")
                    
                    return {
                        'coupon_rate': coupon_rate,
                        'par_value': par_value,
                        'issue_date': issue_date,
                        'maturity_date': maturity_date
                    }
            
        except Exception as e:
            logger.warning(f"[WARNING] Could not fetch auction data for {maturity_years}Y: {e}")
        
        # Fallback: estimate from current yields
        logger.info(f"[FALLBACK] Using estimated coupon for {maturity_years}Y based on yields")
        yields = BondParameterFetcher.fetch_treasury_yields()
        
        yield_map = {2: '2Y', 5: '5Y', 10: '10Y', 30: '30Y'}
        yield_key = yield_map.get(maturity_years, '10Y')
        
        return {
            'coupon_rate': yields[yield_key] / 100,
            'par_value': 100.0,
            'issue_date': 'Estimated',
            'maturity_date': 'Estimated'
        }
    
    @staticmethod
    def get_bond_parameters(symbol: str, expiry_date: datetime) -> Dict:
        """
        Get bond parameters based on symbol and expiry
        Fetches REAL coupon rate from recent Treasury auctions
        
        Args:
            symbol: ZN, ZB, ZF, or ZT
            expiry_date: Contract expiry date
            
        Returns:
            Dictionary with bond parameters
        """
        # Calculate years to maturity from expiry
        years_to_maturity = max(0.1, (expiry_date - datetime.now()).days / 365.25)
        
        # Map symbol to nominal maturity
        maturity_map = {
            'ZT': 2,   # 2-Year
            'ZF': 5,   # 5-Year
            'ZN': 10,  # 10-Year
            'ZB': 30   # 30-Year
        }
        
        # Extract base symbol (remove contract month)
        base_symbol = symbol[:2] if len(symbol) >= 2 else symbol
        nominal_maturity = maturity_map.get(base_symbol, 10)
        
        logger.info(f"[FETCHING] Real auction data for {nominal_maturity}Y Treasury...")
        
        # Fetch real auction data from TreasuryDirect
        auction_data = BondParameterFetcher.fetch_current_treasury_auction_data(nominal_maturity)
        
        return {
            'coupon_rate': auction_data['coupon_rate'],
            'par_value': auction_data['par_value'],
            'years_to_maturity': years_to_maturity,
            'nominal_maturity': nominal_maturity,
            'payment_frequency': 2,  # Semi-annual for all US Treasuries
            'issue_date': auction_data.get('issue_date', 'Unknown'),
            'maturity_date_auction': auction_data.get('maturity_date', 'Unknown')
        }


class DynamicPositionSizer:
    """Calculate position sizes based on confidence"""
    
    def __init__(self, min_pct: float = 1.0, max_pct: float = 5.0, min_conf: float = 20.0):
        self.min_position_pct = min_pct
        self.max_position_pct = max_pct
        self.min_confidence = min_conf
    
    def calculate_position_size(self, equity: float, confidence: float, price: float) -> Dict:
        """Calculate position size based on confidence (20-100%)"""
        
        if confidence < self.min_confidence:
            return {'can_trade': False, 'reason': f'Low confidence ({confidence:.1f}%)', 'quantity': 0}
        
        if equity <= 0:
            return {'can_trade': False, 'reason': 'No equity', 'quantity': 0}
        
        # Linear interpolation
        normalized = (confidence - self.min_confidence) / (100 - self.min_confidence)
        normalized = max(0, min(1, normalized))
        
        position_pct = self.min_position_pct + (self.max_position_pct - self.min_position_pct) * normalized
        position_value = equity * (position_pct / 100)
        
        # Treasury futures: 1 contract = $1000 face value
        quantity = max(1, int(position_value / (price * 1000)))
        
        actual_value = quantity * price * 1000
        actual_pct = (actual_value / equity) * 100
        
        return {
            'can_trade': True,
            'quantity': quantity,
            'position_pct': position_pct,
            'actual_pct': actual_pct,
            'value': actual_value,
            'reason': f'{confidence:.1f}% conf -> {position_pct:.1f}% position'
        }


class ContinuousTreasuryBot:
    """
    Continuous Treasury arbitrage bot
    - Monitors front month contracts
    - Auto-rolls to next contract on expiry
    - Fetches fresh parameters on rollover
    - Runs until TWS closes
    """
    
    def __init__(self, 
                 ib_host: str = '127.0.0.1',
                 ib_port: int = 7497,
                 arbitrage_threshold: float = 0.0001,
                 scan_interval: int = 5):  # 5 secondes
        """
        Args:
            ib_host: IB TWS host
            ib_port: Port (7497=Paper, 7496=Live)
            arbitrage_threshold: Min mispricing to trade (0.01%)
            scan_interval: Seconds between scans (5 = 5 secondes)
        """
        self.ib = IB()
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.arbitrage_threshold = arbitrage_threshold
        self.scan_interval = scan_interval
        
        self.position_sizer = DynamicPositionSizer(min_pct=1.0, max_pct=5.0)
        self.bond_fetcher = BondParameterFetcher()
        
        self.account_equity = 0.0
        self.account_number = None
        self.active_contracts = {}  # Track current front month contracts
        self.running = False
    
    async def connect(self) -> bool:
        """Connect to TWS"""
        try:
            await self.ib.connectAsync(self.ib_host, self.ib_port, clientId=1)
            logger.info(f"[CONNECTED] Connected to IB TWS at {self.ib_host}:{self.ib_port}")
            await self.update_account_info()
            return True
        except Exception as e:
            logger.error(f"[ERROR] Connection failed: {e}")
            return False
    
    async def update_account_info(self):
        """Get account equity and positions"""
        try:
            accounts = self.ib.managedAccounts()
            if accounts:
                self.account_number = accounts[0]
            
            # Get equity
            account_values = self.ib.accountValues()
            for av in account_values:
                if av.tag == 'NetLiquidation':
                    if av.currency == 'USD':
                        self.account_equity = float(av.value)
                        logger.info(f"[EQUITY] ${self.account_equity:,.2f} USD")
                        break
                    elif av.currency == 'EUR':
                        self.account_equity = float(av.value) * 1.08
                        logger.info(f"[EQUITY] €{float(av.value):,.2f} EUR ≈ ${self.account_equity:,.2f} USD")
                        break
            
            if self.account_equity == 0:
                logger.warning("[WARNING] Equity = $0, using $1M default")
                self.account_equity = 1000000.0
                
        except Exception as e:
            logger.error(f"[ERROR] Account update failed: {e}")
    
    async def get_front_month_contract(self, symbol: str) -> Optional[Dict]:
        """
        Get front month (nearest expiry) contract for a symbol
        Always selects the most liquid contract
        """
        try:
            contract = Future(symbol=symbol, exchange='CBOT', currency='USD')
            details = await self.ib.reqContractDetailsAsync(contract)
            
            if not details:
                return None
            
            # Get all available contracts
            contracts = [cd.contract for cd in details]
            
            # Sort by expiry (ascending) and select front month
            sorted_contracts = sorted(contracts, key=lambda c: c.lastTradeDateOrContractMonth)
            front_month = sorted_contracts[0]
            
            # Parse expiry date
            expiry_str = front_month.lastTradeDateOrContractMonth
            expiry_date = datetime.strptime(expiry_str[:8], '%Y%m%d')
            
            # Check if contract is about to expire (within 7 days)
            days_to_expiry = (expiry_date - datetime.now()).days
            
            if days_to_expiry < 7:
                logger.info(f"[ROLLOVER] {front_month.localSymbol} expires in {days_to_expiry} days, will roll to next contract")
                # Select next contract if current is expiring soon
                if len(sorted_contracts) > 1:
                    front_month = sorted_contracts[1]
                    expiry_str = front_month.lastTradeDateOrContractMonth
                    expiry_date = datetime.strptime(expiry_str[:8], '%Y%m%d')
                    days_to_expiry = (expiry_date - datetime.now()).days
            
            # Fetch fresh bond parameters
            bond_params = self.bond_fetcher.get_bond_parameters(front_month.symbol, expiry_date)
            
            logger.info(f"[CONTRACT] {front_month.localSymbol} - Expiry: {expiry_date.strftime('%Y-%m-%d')} ({days_to_expiry} days)")
            logger.info(f"[PARAMS] Coupon: {bond_params['coupon_rate']*100:.2f}%, Maturity: {bond_params['years_to_maturity']:.1f}Y")
            
            return {
                'contract': front_month,
                'symbol': front_month.symbol,
                'local_symbol': front_month.localSymbol,
                'expiry_date': expiry_date,
                'days_to_expiry': days_to_expiry,
                'bond_params': bond_params
            }
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to get {symbol} contract: {e}")
            return None
    
    def calculate_npv(self, params: Dict, discount_rate: float) -> float:
        """Calculate Net Present Value"""
        par = params['par_value']
        coupon = params['coupon_rate']
        years = params['years_to_maturity']
        freq = params['payment_frequency']
        
        coupon_payment = (par * coupon) / freq
        n_periods = int(years * freq)
        period_rate = discount_rate / freq
        
        if n_periods == 0:
            return par
        
        # NPV of coupons
        npv_coupons = sum(coupon_payment / ((1 + period_rate) ** t) for t in range(1, n_periods + 1))
        
        # NPV of par value
        npv_par = par / ((1 + period_rate) ** n_periods)
        
        return npv_coupons + npv_par
    
    async def analyze_and_trade(self, contract_info: Dict):
        """Analyze contract and execute trades"""
        contract = contract_info['contract']
        params = contract_info['bond_params']
        
        try:
            # Request market data (real-time first)
            self.ib.reqMktData(contract, '', False, False)
            await asyncio.sleep(2)
            
            ticker = self.ib.ticker(contract)
            
            # Try to get real-time price
            price = None
            if ticker.last and not np.isnan(ticker.last) and ticker.last > 0:
                price = ticker.last
                price_type = "Real-Time"
            elif ticker.close and not np.isnan(ticker.close) and ticker.close > 0:
                price = ticker.close
                price_type = "Close"
            elif ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
                price = (ticker.bid + ticker.ask) / 2
                price_type = "Bid/Ask"
            
            # If no real-time data, try delayed data
            if price is None:
                logger.info(f"[DELAYED DATA] Real-time not available for {contract_info['local_symbol']}, trying delayed data...")
                
                # Cancel previous request
                self.ib.cancelMktData(contract)
                await asyncio.sleep(0.5)
                
                # Request delayed data
                self.ib.reqMktData(contract, '233', False, False)
                await asyncio.sleep(3)
                
                ticker = self.ib.ticker(contract)
                
                # Check all possible delayed fields
                if hasattr(ticker, 'delayedLast') and ticker.delayedLast and ticker.delayedLast > 0:
                    price = ticker.delayedLast
                    price_type = "Delayed Last"
                elif hasattr(ticker, 'delayedBid') and hasattr(ticker, 'delayedAsk') and ticker.delayedBid and ticker.delayedAsk and ticker.delayedBid > 0:
                    price = (ticker.delayedBid + ticker.delayedAsk) / 2
                    price_type = "Delayed Bid/Ask"
                elif ticker.last and not np.isnan(ticker.last) and ticker.last > 0:
                    price = ticker.last
                    price_type = "Delayed (auto)"
                elif ticker.close and not np.isnan(ticker.close) and ticker.close > 0:
                    price = ticker.close
                    price_type = "Delayed Close"
            
            # If STILL no price, use estimated price from contract details
            if price is None:
                logger.warning(f"[NO MARKET DATA] {contract_info['local_symbol']} - Using estimated price from fundamentals")
                
                # Estimate price from NPV as fallback
                discount_rate = params['coupon_rate']
                npv = self.calculate_npv(params, discount_rate)
                
                # Use NPV as proxy price (will show 0% mispricing but allows position tracking)
                price = npv
                price_type = "Estimated (NPV)"
                
                logger.info(f"[ESTIMATED PRICE] {contract_info['local_symbol']}: ${price:.2f} (based on NPV calculation)")
            
            logger.info(f"[PRICE SOURCE] {contract_info['local_symbol']}: ${price:.4f} ({price_type})")
            
            # Calculate NPV using coupon rate as discount rate
            discount_rate = params['coupon_rate']
            npv = self.calculate_npv(params, discount_rate)
            
            # Calculate mispricing
            diff = npv - price
            pct_diff = (diff / npv) * 100
            
            # Calculate confidence
            confidence = min(abs(pct_diff) * 10, 100)
            
            # Reduce confidence if using estimated price
            if price_type == "Estimated (NPV)":
                confidence = 0  # Don't trade on estimated prices
                logger.warning(f"[WARNING] Trading disabled for {contract_info['local_symbol']} - No real market data available")
            
            # Adjust for maturity preference
            if params['years_to_maturity'] < 2:
                confidence *= 0.8
            elif params['years_to_maturity'] > 20:
                confidence *= 0.9
            
            # Determine signal
            signal = 'HOLD'
            if pct_diff > self.arbitrage_threshold * 100:
                signal = 'BUY'
            elif pct_diff < -self.arbitrage_threshold * 100:
                signal = 'SELL'
            
            # Calculate position size
            sizing = self.position_sizer.calculate_position_size(self.account_equity, confidence, price)
            
            # Log analysis
            logger.info(f"\n{'='*80}")
            logger.info(f"[{contract_info['local_symbol']}] Analysis")
            logger.info(f"{'='*80}")
            logger.info(f"Price: ${price:.4f} ({price_type}) | NPV: ${npv:.2f}")
            logger.info(f"Mispricing: {pct_diff:+.2f}% | Signal: {signal}")
            logger.info(f"Confidence: {confidence:.1f}%")
            logger.info(f"Days to Expiry: {contract_info['days_to_expiry']}")
            
            if sizing['can_trade']:
                logger.info(f"Position: {sizing['quantity']} contracts ({sizing['actual_pct']:.2f}% / ${sizing['value']:,.0f})")
            else:
                logger.info(f"No Trade: {sizing['reason']}")
            
            # Execute trade
            if signal != 'HOLD' and sizing['can_trade']:
                await self.execute_trade(contract, signal, sizing, price, pct_diff, confidence)
            
            # Clean up market data subscription
            self.ib.cancelMktData(contract)
                
        except Exception as e:
            logger.error(f"[ERROR] Analysis failed for {contract_info['local_symbol']}: {e}")
            import traceback
            traceback.print_exc()
    
    async def execute_trade(self, contract, signal, sizing, price, mispricing, confidence):
        """Place order on IB"""
        action = 'BUY' if signal == 'BUY' else 'SELL'
        quantity = sizing['quantity']
        
        try:
            limit_price = price * (1.001 if action == 'BUY' else 0.999)
            order = LimitOrder(action, quantity, limit_price)
            
            trade = self.ib.placeOrder(contract, order)
            
            logger.info(f"\n{'='*80}")
            logger.info(f"[ORDER] {action} {contract.localSymbol}")
            logger.info(f"{'='*80}")
            logger.info(f"Quantity: {quantity} contracts @ ${limit_price:.4f}")
            logger.info(f"Value: ${sizing['value']:,.2f} ({sizing['actual_pct']:.2f}% of equity)")
            logger.info(f"Reason: {mispricing:+.2f}% mispricing | {confidence:.1f}% confidence")
            logger.info(f"{'='*80}")
            
        except Exception as e:
            logger.error(f"[ERROR] Order failed: {e}")
    
    async def run_continuous(self):
        """
        Main loop - runs continuously until TWS closes
        Scans every N seconds, auto-rolls contracts on expiry
        """
        self.running = True
        
        logger.info("\n" + "="*80)
        logger.info("[BOT START] Continuous Treasury Arbitrage Bot")
        logger.info("="*80)
        logger.info(f"Scan Interval: {self.scan_interval}s ({self.scan_interval/60:.1f} minutes)")
        logger.info(f"Arbitrage Threshold: {self.arbitrage_threshold*100}%")
        logger.info(f"Position Range: 1-5% of equity")
        logger.info("="*80)
        
        # Symbols to trade (front month only)
        symbols = ['ZN', 'ZB', 'ZF', 'ZT']  # Tous les Treasuries
        
        scan_count = 0
        
        try:
            while self.running:
                scan_count += 1
                logger.info(f"\n[SCAN #{scan_count}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Update account
                await self.update_account_info()
                
                # Scan and trade each symbol
                for symbol in symbols:
                    # Get front month contract (auto-rolls if expiring)
                    contract_info = await self.get_front_month_contract(symbol)
                    
                    if contract_info:
                        # Store active contract
                        self.active_contracts[symbol] = contract_info
                        
                        # Analyze and trade
                        await self.analyze_and_trade(contract_info)
                    
                    await asyncio.sleep(1)  # Rate limiting
                
                # Wait before next scan
                logger.info(f"\n[SLEEP] Next scan in {self.scan_interval}s...")
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
    
    bot = ContinuousTreasuryBot(
        ib_host='127.0.0.1',
        ib_port=7497,  # Paper Trading
        arbitrage_threshold=0.0001,  # 0.01%
        scan_interval=5  # 5 secondes
    )
    
    # Connect
    if not await bot.connect():
        logger.error("[FAILED] Could not connect to TWS")
        logger.info("\nTroubleshooting:")
        logger.info("1. Is TWS open?")
        logger.info("2. Is API enabled? (File -> Global Configuration -> API)")
        logger.info("3. Port correct? (7497=Paper, 7496=Live)")
        return
    
    # Run continuously
    try:
        await bot.run_continuous()
    except KeyboardInterrupt:
        logger.info("\n[CTRL+C] Shutting down...")
        bot.running = False


if __name__ == "__main__":
    asyncio.run(main())