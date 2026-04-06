#!/usr/bin/env python3
"""
bonds.py — Scorpio entry point

Usage:
    python bonds.py help
    python bonds.py start
    python bonds.py start --port 7496
    python bonds.py backtest --from 2019-01-01 --html
    python bonds.py report --html
    python bonds.py positions
    python bonds.py spreads
"""

from bonds_arbitrage.cli import main

if __name__ == '__main__':
    main()
