"""
Latency Arbitrage Strategy (Kalshi-only)
Exploits the delay between crypto spot price movements and
Kalshi order book updates for 5-min / 15-min recurring markets.
"""

import asyncio
import logging
import time
from typing import Optional

from config import Config
from models import (
    Signal, Side, Platform, ContractSide, SpotPriceUpdate, OrderBook,
)
from data.aggregator import DataAggregator

logger = logging.getLogger(__name__)


class LatencyArbStrategy:
    """
    Monitors Coinbase for rapid BTC/ETH price movements and generates
    signals to sweep the corresponding 15-minute YES/NO prediction
    markets on Kalshi before they adjust.
    """

    def __init__(self, aggregator: DataAggregator):
        self.aggregator = aggregator
        self.jump_threshold = Config.SPOT_PRICE_JUMP_THRESHOLD
        self.min_edge = Config.MIN_EDGE_THRESHOLD
        self._min_entry_price = max(0.0, Config.LATENCY_MIN_ENTRY_PRICE)
        self._max_entry_price = min(1.0, Config.LATENCY_MAX_ENTRY_PRICE)
        self._active = False

        # Cooldown tracking to avoid re-entering the same move
        self._last_signal_time: dict[str, float] = {}
        self._cooldown_seconds = 30.0

        # Market Series mappings: maps spot symbol to Kalshi Series Tickers
        self.market_mappings: dict[str, dict] = {
            "BTCUSDT": {
                "series": "KXBTC15M", # Default to 15-min BTC
                "active_ticker": None,
            },
            "ETHUSDT": {
                "series": "KXETH15M", # Default to 15-min ETH
                "active_ticker": None,
            },
        }

    def configure_markets(self, mappings: dict[str, dict]):
        """Set the series tickers at runtime."""
        self.market_mappings.update(mappings)
        logger.info("Latency arb: configured %d symbol mappings", len(mappings))

    async def start(self):
        """Register with the aggregator and begin monitoring."""
        self._active = True
        self.aggregator.binance.on_update(self._on_spot_update)
        
        # Start the ticker discovery loop
        asyncio.create_task(self._discovery_loop())
        
        logger.info(
            "Latency arb: started (threshold=$%.2f, cooldown=%.0fs)",
            self.jump_threshold, self._cooldown_seconds,
        )

    async def stop(self):
        self._active = False

    async def _discovery_loop(self):
        """Periodically refresh the active tickers for each series."""
        while self._active:
            try:
                for symbol, data in self.market_mappings.items():
                    series = data.get("series")
                    if series:
                        new_ticker = await self.aggregator.kalshi.get_active_series_ticker(series)
                        if new_ticker and new_ticker != data.get("active_ticker"):
                            logger.info("Latency arb: updated %s active ticker to %s", symbol, new_ticker)
                            data["active_ticker"] = new_ticker
            except Exception as e:
                logger.error("Ticker discovery error: %s", e)
            
            await asyncio.sleep(60) # check every minute

    async def _on_spot_update(
        self,
        update: SpotPriceUpdate,
        delta: Optional[float],
    ):
        """Called on every price update. Acts on jumps."""
        if not self._active or delta is None:
            return

        symbol = update.symbol
        now = time.time()

        # Check cooldown
        last = self._last_signal_time.get(symbol, 0)
        if now - last < self._cooldown_seconds:
            return

        data = self.market_mappings.get(symbol, {})
        active_ticker = data.get("active_ticker")
        if not active_ticker:
            return

        # Determine direction
        if delta > 0:
            signals = self._generate_up_signals(symbol, delta, active_ticker)
        else:
            signals = self._generate_down_signals(symbol, delta, active_ticker)

        if signals:
            self._last_signal_time[symbol] = now
            return signals

    def _generate_up_signals(
        self, symbol: str, delta: float, ticker: str
    ) -> list[Signal]:
        """Rapid move UP: Buy YES."""
        signals = []
        confidence = min(abs(delta) / (self.jump_threshold * 3), 0.95)

        ob = self.aggregator.get_order_book(ticker)
        if ob and ob.best_ask is not None:
            if not (self._min_entry_price <= ob.best_ask <= self._max_entry_price):
                logger.debug(
                    "Latency arb: skip %s YES ask %.4f outside [%.2f, %.2f]",
                    ticker,
                    ob.best_ask,
                    self._min_entry_price,
                    self._max_entry_price,
                )
                return signals

            # Estimate edge for BUYing YES
            edge = self._estimate_edge(ob.best_ask, direction="up", delta=delta)
            if edge >= self.min_edge:
                signals.append(Signal(
                    strategy="latency_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=ob.best_ask,
                    quantity=self._size_for_edge(edge),
                    confidence=confidence,
                    reason=f"{symbol} +${delta:.2f} jump, Kalshi YES ask={ob.best_ask:.4f}, edge={edge:.4f}",
                ))

        return signals

    def _generate_down_signals(
        self, symbol: str, delta: float, ticker: str
    ) -> list[Signal]:
        """Rapid move DOWN: Buy NO."""
        signals = []
        confidence = min(abs(delta) / (self.jump_threshold * 3), 0.95)

        ob = self.aggregator.get_order_book(ticker)
        if ob and ob.best_bid is not None:
            # Buying NO at (1 - YES_bid)
            # If YES Bid is 0.40, price to buy NO is 0.60
            no_ask = 1.0 - ob.best_bid
            if not (self._min_entry_price <= no_ask <= self._max_entry_price):
                logger.debug(
                    "Latency arb: skip %s NO ask %.4f outside [%.2f, %.2f]",
                    ticker,
                    no_ask,
                    self._min_entry_price,
                    self._max_entry_price,
                )
                return signals

            edge = self._estimate_edge(no_ask, direction="down", delta=delta)
            
            if edge >= self.min_edge:
                signals.append(Signal(
                    strategy="latency_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.NO,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=no_ask,
                    quantity=self._size_for_edge(edge),
                    confidence=confidence,
                    reason=f"{symbol} -${abs(delta):.2f} drop, Kalshi NO ask={no_ask:.4f} (from YES bid={ob.best_bid:.4f}), edge={edge:.4f}",
                ))

        return signals

    def _estimate_edge(self, current_ask: float, direction: str, delta: float) -> float:
        """Estimate the edge vs fair value."""
        delta_abs = abs(delta)
        if delta_abs >= 200:
            fair_value = 0.85
        elif delta_abs >= 150:
            fair_value = 0.78
        elif delta_abs >= 100:
            fair_value = 0.70
        elif delta_abs >= 75:
            fair_value = 0.62
        else:
            fair_value = 0.55

        return max(fair_value - current_ask, 0.0)

    def _size_for_edge(self, edge: float) -> int:
        if edge >= 0.15: return 100
        if edge >= 0.10: return 50
        return 20
