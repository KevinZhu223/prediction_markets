"""
Latency Arbitrage Strategy (Kalshi-only)
Exploits the delay between crypto spot price movements and
Kalshi order book updates for 15-min recurring markets.

KXBTC15M / KXETH15M markets are directional: "Will BTC be higher at
the end of the 15 minutes than at the start?"  The market has a
`floor_strike` field that is the opening reference price.  YES wins
if the 1-minute BRTI average at close >= floor_strike.

The OLD logic only reacted to local micro-jumps which caused the bot
to buy YES during a brief bounce even when spot was still well below
the strike.  The NEW logic checks current spot vs floor_strike.
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
    signals for the corresponding 15-minute directional prediction
    markets on Kalshi.

    Key improvement: the bot now tracks the floor_strike (interval
    opening price) from Kalshi's market data and only generates signals
    in the correct direction relative to the strike.
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
                "floor_strike": None,  # Opening reference price
            },
            "ETHUSDT": {
                "series": "KXETH15M", # Default to 15-min ETH
                "active_ticker": None,
                "floor_strike": None,
            },
        }

        # Cache the latest spot price per symbol for strike comparison
        self._latest_spot: dict[str, float] = {}

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
        """Periodically refresh the active tickers and floor_strike for each series."""
        while self._active:
            try:
                for symbol, data in self.market_mappings.items():
                    series = data.get("series")
                    if series:
                        new_ticker = await self.aggregator.kalshi.get_active_series_ticker(series)
                        if new_ticker and new_ticker != data.get("active_ticker"):
                            logger.info("Latency arb: updated %s active ticker to %s", symbol, new_ticker)
                            data["active_ticker"] = new_ticker

                        # Fetch the floor_strike (opening reference price) for the active market
                        if new_ticker:
                            try:
                                market_detail = await self.aggregator.kalshi.get_market(new_ticker)
                                if market_detail:
                                    fs = market_detail.get("floor_strike")
                                    if fs is not None:
                                        data["floor_strike"] = float(fs)
                                        logger.info(
                                            "Latency arb: %s floor_strike=$%.2f",
                                            new_ticker, data["floor_strike"],
                                        )
                            except Exception as e:
                                logger.debug("Latency arb: floor_strike fetch error for %s: %s", new_ticker, e)
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

        # Always track the latest spot price
        self._latest_spot[symbol] = update.price

        # Check cooldown
        last = self._last_signal_time.get(symbol, 0)
        if now - last < self._cooldown_seconds:
            return

        data = self.market_mappings.get(symbol, {})
        active_ticker = data.get("active_ticker")
        floor_strike = data.get("floor_strike")
        if not active_ticker or floor_strike is None:
            return

        # Core fix: determine the TRUE direction relative to the interval strike.
        # The market resolves YES if spot >= floor_strike at expiry.
        spot = update.price
        distance_from_strike = spot - floor_strike

        # Only generate signals when the jump agrees with the strike direction.
        if delta > 0 and distance_from_strike > 0:
            # Price jumped UP and we're above the strike → YES is likely
            signals = self._generate_up_signals(symbol, delta, active_ticker, spot, floor_strike)
        elif delta < 0 and distance_from_strike < 0:
            # Price dropped DOWN and we're below the strike → NO is likely
            signals = self._generate_down_signals(symbol, delta, active_ticker, spot, floor_strike)
        else:
            # Jump direction disagrees with strike position — skip
            return

        if signals:
            self._last_signal_time[symbol] = now
            return signals

    def _generate_up_signals(
        self, symbol: str, delta: float, ticker: str,
        spot: float, floor_strike: float,
    ) -> list[Signal]:
        """Spot > strike after an UP jump: Buy YES."""
        signals = []
        distance = spot - floor_strike  # positive when above strike
        confidence = min(0.95, 0.50 + (distance / floor_strike) * 200)

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

            # Estimate edge based on distance from strike, not just micro-jump
            edge = self._estimate_edge(ob.best_ask, distance=distance, strike=floor_strike)
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
                    reason=(
                        f"{symbol} spot=${spot:,.2f} > strike=${floor_strike:,.2f} "
                        f"(+${distance:,.2f}), jump=${delta:,.2f}, "
                        f"YES ask={ob.best_ask:.4f}, edge={edge:.4f}"
                    ),
                ))

        return signals

    def _generate_down_signals(
        self, symbol: str, delta: float, ticker: str,
        spot: float, floor_strike: float,
    ) -> list[Signal]:
        """Spot < strike after a DOWN jump: Buy NO."""
        signals = []
        distance = floor_strike - spot  # positive when below strike
        confidence = min(0.95, 0.50 + (distance / floor_strike) * 200)

        ob = self.aggregator.get_order_book(ticker)
        if ob and ob.best_bid is not None:
            # Buying NO at (1 - YES_bid)
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

            edge = self._estimate_edge(no_ask, distance=distance, strike=floor_strike)
            
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
                    reason=(
                        f"{symbol} spot=${spot:,.2f} < strike=${floor_strike:,.2f} "
                        f"(-${distance:,.2f}), jump=${delta:,.2f}, "
                        f"NO ask={no_ask:.4f}, edge={edge:.4f}"
                    ),
                ))

        return signals

    def _estimate_edge(self, current_ask: float, *, distance: float, strike: float) -> float:
        """
        Estimate the edge vs fair value based on distance from the floor_strike.

        distance: absolute $ distance between spot and strike (always positive).
        strike:   the floor_strike value, used for normalisation.
        """
        if strike <= 0:
            return 0.0

        pct_distance = distance / strike  # e.g. $200 / $80000 = 0.25%

        # Map distance-from-strike percentage to an estimated fair probability
        if pct_distance >= 0.005:      # >=0.5% away from strike
            fair_value = 0.88
        elif pct_distance >= 0.003:    # >=0.3%
            fair_value = 0.80
        elif pct_distance >= 0.002:    # >=0.2%
            fair_value = 0.72
        elif pct_distance >= 0.001:    # >=0.1%
            fair_value = 0.62
        else:
            fair_value = 0.55

        return max(fair_value - current_ask, 0.0)

    def _size_for_edge(self, edge: float) -> int:
        if edge >= 0.15: return 100
        if edge >= 0.10: return 50
        return 20
