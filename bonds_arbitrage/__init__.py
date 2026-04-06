"""
bonds_arbitrage — Government Bond Spread Arbitrage Strategy (Scorpio v2.0)
"""

from .config import *
from .data import SovereignYieldFetcher
from .analytics import YieldCurveBuilder, BondPricer, SpreadAnalyzer
from .signals import SignalAggregator
from .risk import RiskManager, DynamicPositionSizer
from .portfolio import PortfolioTracker
from .backtest import Backtester
from .reports import ReportGenerator
from .bot import GovernmentBondsBot

__all__ = [
    'SovereignYieldFetcher',
    'YieldCurveBuilder', 'BondPricer', 'SpreadAnalyzer',
    'SignalAggregator',
    'RiskManager', 'DynamicPositionSizer',
    'PortfolioTracker',
    'Backtester',
    'ReportGenerator',
    'GovernmentBondsBot',
]
