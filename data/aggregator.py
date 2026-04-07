"""
Data Aggregator
Synchronizes crypto spot price feeds with order books from
Kalshi, providing a unified view for strategies.
"""

import asyncio
import logging
import time
from typing import Optional

from models import OrderBook, SpotPriceUpdate, Platform
from exchanges.base import ExchangeBase
from data.crypto_feed import CryptoFeed

logger = logging.getLogger(__name__)


class DataAggregator:
    """
    Maintains synchronized state across all data sources.
    Strategies query this for the latest prices instead of
    hitting APIs directly.
    """

    def __init__(
        self,
        kalshi: ExchangeBase,
        binance: CryptoFeed,
    ):
        self.kalshi = kalshi
        self.binance = binance

        # Latest order books indexed by market_id
        self._order_books: dict[str, OrderBook] = {}

        # Latest spot prices indexed by symbol
        self._spot_prices: dict[str, SpotPriceUpdate] = {}

        # Callbacks
        self._ob_callbacks = []
        self._spot_callbacks = []

    def on_orderbook_update(self, callback):
        self._ob_callbacks.append(callback)

    def on_spot_update(self, callback):
        self._spot_callbacks.append(callback)

    # ── Order book management ───────────────────────────────────────────

    async def _handle_ob_update(self, ob: OrderBook):
        self._order_books[ob.market_id] = ob
        for cb in self._ob_callbacks:
            try:
                await cb(ob)
            except Exception as e:
                logger.error("OB callback error: %s", e)

    def get_order_book(self, market_id: str) -> Optional[OrderBook]:
        return self._order_books.get(market_id)

    def get_all_order_books(self) -> dict[str, OrderBook]:
        return dict(self._order_books)

    # ── Spot price management ───────────────────────────────────────────

    async def _handle_spot_update(self, update: SpotPriceUpdate, delta: Optional[float]):
        self._spot_prices[update.symbol] = update
        for cb in self._spot_callbacks:
            try:
                await cb(update, delta)
            except Exception as e:
                logger.error("Spot callback error: %s", e)

    def get_spot_price(self, symbol: str) -> Optional[float]:
        update = self._spot_prices.get(symbol.upper())
        return update.price if update else None

    # ── Polling loop for Kalshi order books ─────────────────────────────

    async def poll_order_books(
        self,
        market_ids: list[str],
        interval: float = 2.0,
    ):
        """
        Periodically fetch order books for configured Kalshi markets.
        """
        while True:
            for market_id in market_ids:
                try:
                    ob = await self.kalshi.fetch_order_book(market_id)
                    await self._handle_ob_update(ob)
                except Exception as e:
                    logger.debug("Kalshi OB poll error for %s: %s", market_id, e)

            await asyncio.sleep(interval)

    # ── Start streaming ─────────────────────────────────────────────────

    async def start_streams(
        self,
        kalshi_markets: list[str] = None,
    ):
        """
        Start WebSocket streams for Kalshi + Binance.
        """
        tasks = []

        # Binance spot feed
        self.binance.on_update(self._handle_spot_update)
        tasks.append(asyncio.create_task(self.binance.start()))

        # Kalshi WS
        if kalshi_markets:
            tasks.append(asyncio.create_task(
                self.kalshi.stream_order_book(kalshi_markets, self._handle_ob_update)
            ))

        logger.info("Aggregator: started %d data streams", len(tasks))
        return tasks
