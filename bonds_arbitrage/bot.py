"""
Bot — GovernmentBondsBot (IBKR live trading)

Cycle:
  • Rebalancing quotidien (REBALANCE_HOUR UTC) : nouveaux signaux + cancel/reset TP/SL
  • Monitoring continu (MONITOR_INTERVAL s)    : vérification TP/SL uniquement
"""

import asyncio
import logging
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional

from .config import (
    IB_HOST, IB_PORT, DRY_RUN, REBALANCE_HOUR, MONITOR_INTERVAL,
    DATA_REFRESH_H, ZSCORE_EXIT, ZSCORE_ENTRY, SL_ZSCORE_EXTEND, MIN_CONFIDENCE,
)
from .data import SovereignYieldFetcher
from .analytics import YieldCurveBuilder, BondPricer, SpreadAnalyzer
from .signals import SignalAggregator
from .risk import RiskManager, DynamicPositionSizer
from .portfolio import PortfolioTracker
from .reports import ReportGenerator

logger = logging.getLogger(__name__)


class GovernmentBondsBot:
    """
    Live trading bot for government bond spread arbitrage.

    Rebalancing logic (daily):
      1. Fetch current spreads + z-scores
      2. Close positions where z has reverted (|z| < ZSCORE_EXIT)
      3. Cancel + recalculate TP/SL on remaining positions
      4. Open new positions on fresh signals
      5. Place TP/SL as OCA bracket orders in IBKR
    """

    def __init__(self, ib_host: str = IB_HOST, ib_port: int = IB_PORT):
        from ib_insync import IB, Future
        self.IB     = IB
        self.Future = Future
        self.ib     = IB()
        self.host   = ib_host
        self.port   = ib_port

        self.fetcher    = SovereignYieldFetcher(history_days=400)
        self.curve      = YieldCurveBuilder(self.fetcher)
        self.pricer     = BondPricer(self.curve)
        self.spread_ana = SpreadAnalyzer(self.fetcher)
        self.aggregator = SignalAggregator(self.pricer)
        self.risk_mgr   = RiskManager()
        self.portfolio  = PortfolioTracker()
        self.sizer      = DynamicPositionSizer()
        self.reporter   = ReportGenerator()

        self.equity             = 0.0
        self.running            = False
        self.last_rebalance:    Optional[datetime] = None
        self.last_data_refresh: Optional[datetime] = None

    # ── Connection ────────────────────────────────────────────────────

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

    # ── Contracts / Prices ────────────────────────────────────────────

    async def _front_month(self, symbol: str, exchange: str):
        try:
            raw     = self.Future(symbol=symbol, exchange=exchange, currency='USD')
            details = await self.ib.reqContractDetailsAsync(raw)
            if not details:
                return None
            details.sort(key=lambda d: d.contract.lastTradeDateOrContractMonth)
            front  = details[0].contract
            expiry = datetime.strptime(
                front.lastTradeDateOrContractMonth[:8], '%Y%m%d')
            if (expiry - datetime.now()).days < 7 and len(details) > 1:
                front = details[1].contract
            return front
        except Exception as e:
            logger.debug(f'[CONTRACT] {symbol}: {e}')
            return None

    async def _live_price(self, contract) -> Optional[float]:
        try:
            self.ib.reqMktData(contract, '', False, False)
            await asyncio.sleep(2)
            t = self.ib.ticker(contract)
            for v in [
                t.last, t.close,
                (t.bid + t.ask) / 2 if t.bid and t.ask and t.bid > 0 else None,
            ]:
                if v and not np.isnan(v) and v > 0:
                    self.ib.cancelMktData(contract)
                    return float(v)
            self.ib.cancelMktData(contract)
        except Exception:
            pass
        return None

    async def _fetch_all_prices(self) -> Dict[str, Optional[float]]:
        prices: Dict[str, Optional[float]] = {}
        for (_, __), sym in self.aggregator.COUNTRY_TO_FUTURES.items():
            if sym in prices:
                continue
            exch     = self.aggregator.FUTURES_EXCHANGE.get(sym, 'SMART')
            contract = await self._front_month(sym, exch)
            if contract:
                prices[sym] = await self._live_price(contract)
                logger.info(f'  {sym:6s}: {prices[sym]}')
            else:
                prices[sym] = None
            await asyncio.sleep(0.3)
        return prices

    # ── Orders ────────────────────────────────────────────────────────

    async def _cancel_order(self, order_id):
        if order_id is None:
            return
        try:
            for o in self.ib.openOrders():
                if o.orderId == order_id:
                    self.ib.cancelOrder(o)
                    return
        except Exception as e:
            logger.debug(f'[CANCEL] {order_id}: {e}')

    async def _place_tp_sl(self, contract, action_entry: str, qty: int,
                           tp_price: float, sl_price: float) -> Dict:
        from ib_insync import LimitOrder, StopOrder
        close_action = 'SELL' if action_entry == 'BUY' else 'BUY'
        oca_group    = f'OCA_{contract.localSymbol}_{datetime.now().strftime("%H%M%S")}'

        if DRY_RUN:
            logger.info(
                f'[DRY RUN] TP {close_action} {qty}x @ {tp_price:.3f}  '
                f'SL {close_action} {qty}x @ {sl_price:.3f}')
            return {'tp_order_id': None, 'sl_order_id': None}

        tp_ord = LimitOrder(close_action, qty, tp_price)
        sl_ord = StopOrder(close_action, qty, sl_price)
        tp_ord.ocaGroup = oca_group; tp_ord.ocaType = 1
        sl_ord.ocaGroup = oca_group; sl_ord.ocaType = 1

        tp_t = self.ib.placeOrder(contract, tp_ord)
        sl_t = self.ib.placeOrder(contract, sl_ord)
        return {'tp_order_id': tp_t.order.orderId,
                'sl_order_id': sl_t.order.orderId}

    # ── Rebalancing ───────────────────────────────────────────────────

    async def rebalance(self):
        """Full daily rebalancing cycle."""
        logger.info('\n' + '=' * 80)
        logger.info(f'[REBALANCE] {datetime.now():%Y-%m-%d %H:%M:%S}')
        logger.info('=' * 80)

        await self._refresh_equity()
        spread_results = self.spread_ana.analyze_all()
        prices         = await self._fetch_all_prices()

        # ── 1. Close positions that have reverted ─────────────────────
        for pid, pos in list(self.portfolio.positions.items()):
            for sr in spread_results:
                if sr['pair'] != pos['pair']:
                    continue
                if abs(sr['z_score']) < ZSCORE_EXIT:
                    logger.info(f'[CLOSE] {pos["pair"]}  z={sr["z_score"]:+.2f} → TP')
                    for lk in ('leg_long', 'leg_short'):
                        await self._cancel_order(pos[lk].get('tp_order_id'))
                        await self._cancel_order(pos[lk].get('sl_order_id'))
                    ep_l = prices.get(pos['leg_long']['futures'])  or pos['leg_long']['entry_price']
                    ep_s = prices.get(pos['leg_short']['futures']) or pos['leg_short']['entry_price']
                    pnl  = self.portfolio.close_position(pid, ep_l, ep_s, 'TP_ZSCORE')
                    logger.info(f'[CLOSED] {pos["pair"]}  PnL=${pnl:+,.0f}')

        # ── 2. Update TP/SL on remaining positions ────────────────────
        for pid, pos in self.portfolio.positions.items():
            std_now = next(
                (r['spread_std'] for r in spread_results if r['pair'] == pos['pair']),
                0.20)
            trade_proxy = {
                'z_entry':    pos.get('z_entry', ZSCORE_ENTRY),
                'spread_std': std_now,
                'leg_long':  {'price': prices.get(pos['leg_long']['futures']),
                              'npv':   {'dv01': pos['leg_long'].get('dv01', 8.5)}},
                'leg_short': {'price': prices.get(pos['leg_short']['futures']),
                              'npv':   {'dv01': pos['leg_short'].get('dv01', 8.5)}},
            }
            new_levels = self.risk_mgr.compute_tp_sl(trade_proxy)

            for lk, action in (('leg_long', 'BUY'), ('leg_short', 'SELL')):
                leg     = pos[lk]
                new_tp  = new_levels.get(lk, {}).get('tp')
                new_sl  = new_levels.get(lk, {}).get('sl')
                cur_p   = prices.get(leg['futures'])

                if self.risk_mgr.should_update(leg.get('tp'), new_tp, cur_p):
                    logger.info(
                        f'[TP/SL UPDATE] {pid} {lk}: '
                        f'TP {leg.get("tp")} -> {new_tp}  SL {leg.get("sl")} -> {new_sl}')
                    await self._cancel_order(leg.get('tp_order_id'))
                    await self._cancel_order(leg.get('sl_order_id'))
                    if cur_p and new_tp and new_sl:
                        contract = await self._front_month(
                            leg['futures'],
                            self.aggregator.FUTURES_EXCHANGE.get(leg['futures'], 'SMART'))
                        if contract:
                            ids = await self._place_tp_sl(
                                contract, action, leg['qty'], new_tp, new_sl)
                            self.portfolio.update_tp_sl(
                                pid, lk, new_tp, new_sl,
                                ids['tp_order_id'], ids['sl_order_id'])

        # ── 3. Open new positions ─────────────────────────────────────
        open_pairs = {pos['pair'] for pos in self.portfolio.positions.values()}

        for sr in spread_results:
            trade = self.aggregator.build_trade(sr, prices)
            if trade is None or trade['pair'] in open_pairs:
                continue
            if trade['confidence'] < MIN_CONFIDENCE:
                continue

            ref_price = trade['leg_long']['price'] or 100.0
            sizing    = self.sizer.calculate(
                self.equity, trade['confidence'], ref_price, trade['dv01_ratio'])
            if not sizing['can_trade']:
                continue

            logger.info(
                f'\n[NEW TRADE] {trade["pair"]}  {trade["signal"]}\n'
                f'  z={trade["z_score"]:+.2f}  dev={trade["deviation_bps"]:+.1f}bps  '
                f'conf={trade["confidence"]:.0f}%\n'
                f'  LONG  {trade["leg_long"]["futures"]} x{sizing["qty_long"]} '
                f'@ {trade["leg_long"]["price"]}  '
                f'NPV {trade["leg_long"]["npv"]["alpha_pct"]:+.3f}%\n'
                f'  SHORT {trade["leg_short"]["futures"]} x{sizing["qty_short"]} '
                f'@ {trade["leg_short"]["price"]}  '
                f'NPV {trade["leg_short"]["npv"]["alpha_pct"]:+.3f}%'
            )

            tp_sl     = self.risk_mgr.compute_tp_sl(trade)
            order_ids: Dict = {}

            for lk, action in (('leg_long', 'BUY'), ('leg_short', 'SELL')):
                leg = trade[lk]
                qty = sizing['qty_long'] if lk == 'leg_long' else sizing['qty_short']
                contract = await self._front_month(leg['futures'], leg['exchange'])

                if contract and not DRY_RUN:
                    from ib_insync import LimitOrder
                    limit = leg['price'] * (1.001 if action == 'BUY' else 0.999)
                    self.ib.placeOrder(contract, LimitOrder(action, qty, limit))
                elif DRY_RUN:
                    logger.info(f'[DRY RUN] {action} {qty}x {leg["futures"]} @ {leg["price"]}')

                tp = tp_sl.get(lk, {}).get('tp')
                sl = tp_sl.get(lk, {}).get('sl')
                if contract and tp and sl:
                    ids = await self._place_tp_sl(contract, action, qty, tp, sl)
                    tag = lk.replace('leg_', '')
                    order_ids[f'tp_{tag}'] = ids['tp_order_id']
                    order_ids[f'sl_{tag}'] = ids['sl_order_id']

            self.portfolio.open_position(trade, sizing['qty_long'],
                                         sizing['qty_short'], tp_sl, order_ids)
            open_pairs.add(trade['pair'])

        self.last_rebalance = datetime.now()
        self.reporter.print_spreads(spread_results)
        self.reporter.print_positions(self.portfolio)
        self.reporter.print_summary(self.portfolio)

    # ── Main loop ─────────────────────────────────────────────────────

    async def run(self):
        self.running = True
        logger.info('\n' + '=' * 80)
        logger.info('[START] Scorpio — Government Bond Arbitrage v2.0')
        logger.info(f'  Z-score entry/exit/stop : ±{ZSCORE_ENTRY} / ±{ZSCORE_EXIT} / ±{ZSCORE_ENTRY + SL_ZSCORE_EXTEND}')
        logger.info(f'  Rebalancing             : daily at {REBALANCE_HOUR}h UTC')
        logger.info(f'  TP/SL monitor           : every {MONITOR_INTERVAL}s')
        logger.info(f'  Dry run                 : {DRY_RUN}')
        logger.info('=' * 80)

        self.fetcher.fetch_all()
        self.curve.fit_all()
        self.last_data_refresh = datetime.now()

        await self.rebalance()

        try:
            while self.running:
                now = datetime.now()

                if self.last_data_refresh and \
                        (now - self.last_data_refresh).seconds > DATA_REFRESH_H * 3600:
                    logger.info('[DATA REFRESH] Updating sovereign yields...')
                    self.fetcher.fetch_all()
                    self.curve.fit_all()
                    self.last_data_refresh = now

                should_rebal = (
                    self.last_rebalance is None or
                    (now.hour == REBALANCE_HOUR and
                     now.date() > self.last_rebalance.date())
                )
                if should_rebal:
                    await self.rebalance()
                else:
                    logger.info(
                        f'[MONITOR] {now:%H:%M:%S} — '
                        f'{len(self.portfolio.positions)} open positions')

                await asyncio.sleep(MONITOR_INTERVAL)

        except asyncio.CancelledError:
            logger.info('[STOPPED]')
        except Exception as e:
            logger.error(f'[CRASH] {e}')
            import traceback
            traceback.print_exc()
        finally:
            self.running = False
            try:
                self.ib.disconnect()
            except Exception:
                pass
            logger.info('[DISCONNECTED]')
