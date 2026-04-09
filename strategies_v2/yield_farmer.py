"""
Certainty Yield Farmer Strategy

Exploits the "Settlement Delay / Certainty Gap" inefficiency.
Retail traders often sell deciding tickets at $0.95 or $0.96 because they do not
want to wait 1-2 hours for Kalshi to officially process the settlement.
This bot acts as automated "Patience Capital", providing liquidity to these 
traders and capturing the 4-5% spread as a near risk-free yield.
"""

import asyncio
import logging
import time
from typing import Optional

from config import Config
from models import Signal, Side, Platform, ContractSide
from exchanges.kalshi_wrapper import KalshiExchange

logger = logging.getLogger(__name__)


class YieldFarmerStrategy:
    def __init__(self, kalshi: KalshiExchange):
        self.kalshi = kalshi
        self._active = False
        
        # Configuration
        self.min_yield_threshold = 0.95  # Target markets priced >= 95c
        self.max_yield_threshold = 0.98  # Ignore if it's already 99c
        self.poll_interval = 300         # Check every 5 minutes to avoid rate limits
        self._min_volume = max(0.0, Config.YIELD_MIN_VOLUME)
        self._max_spread = max(0.0, Config.YIELD_MAX_SPREAD)
        self._max_hours_to_expiry = max(1.0, Config.YIELD_MAX_HOURS_TO_EXPIRY)
        
        self._active_markets = 0
        self._signals_generated: list[Signal] = []
        self._yields_captured = set()

    async def start(self):
        self._active = True
        logger.info(
            "Yield farmer: started (target range 95c-98c, max_expiry=%.1fh, min_volume=%.0f)",
            self._max_hours_to_expiry,
            self._min_volume,
        )

    async def stop(self):
        self._active = False

    async def run_loop(self, signal_callback=None):
        while self._active:
            try:
                await self._scan_for_yield(signal_callback)
            except Exception as e:
                logger.error("Yield farmer loop error: %s", e)
            
            await asyncio.sleep(self.poll_interval)

    async def _scan_for_yield(self, signal_callback):
        if not self.kalshi:
            return

        all_markets = await self.kalshi.fetch_markets()
        self._active_markets = len(all_markets)

        for m in all_markets:
            ticker = m.get("ticker", "")
            
            # Skip if we already farmed this market
            if ticker in self._yields_captured:
                continue

            hours_to_expiry = self._hours_to_expiry(m)
            if hours_to_expiry is None or hours_to_expiry <= 0:
                continue
            if hours_to_expiry > self._max_hours_to_expiry:
                continue
                
            yes_ask_d = m.get("yes_ask_dollars")
            yes_bid_d = m.get("yes_bid_dollars")
            yes_ask = float(yes_ask_d) if yes_ask_d else None
            yes_bid = float(yes_bid_d) if yes_bid_d else None

            if yes_ask is not None and yes_bid is not None:
                spread = yes_ask - yes_bid
                if spread > self._max_spread:
                    continue
            
            volume = float(m.get("volume_fp", 0) or 0)
            
            # Require at least some volume to avoid dead markets with stale quotes
            if volume < self._min_volume:
                continue

            signal = None

            # Scenario 1: The market is resolved YES, impatient sellers asking 95-98c
            if yes_ask is not None and self.min_yield_threshold <= yes_ask <= self.max_yield_threshold:
                edge = 1.00 - yes_ask
                signal = Signal(
                    strategy="yield_farmer",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=yes_ask,
                    quantity=self._calculate_size(),
                    confidence=0.99, # We only buy near monopolies
                    reason=f"YIELD FARM DOLLAR: Buying YES at {yes_ask:.2f} for guaranteed 1.00 settlement.",
                )

            # Scenario 2: The market is resolved NO, impatient sellers are dumping YES at 2-5c
            elif yes_bid is not None and (1.00 - self.max_yield_threshold) <= yes_bid <= (1.00 - self.min_yield_threshold):
                # We buy the NO contract. price = 1.0 - yes_bid
                no_ask = 1.00 - yes_bid
                if no_ask <= 0 or no_ask >= 1:
                    continue
                signal = Signal(
                    strategy="yield_farmer",
                    action=Side.BUY,
                    contract_side=ContractSide.NO, # We explicitly buy the NO contract
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=no_ask,
                    quantity=self._calculate_size(),
                    confidence=0.99,
                    reason=f"YIELD FARM DOLLAR: Buying NO at {no_ask:.2f} for guaranteed 1.00 settlement.",
                )

            if signal and signal_callback:
                self._yields_captured.add(ticker)
                self._signals_generated.append(signal)
                logger.info(
                    "YIELD FARMER SIGNAL: buy %s %s @ %.2f (locking %.1f%% spread)",
                    signal.contract_side.value, signal.market_id, signal.target_price, (1.00 - signal.target_price)*100
                )
                await signal_callback(signal)

    def _hours_to_expiry(self, market: dict) -> Optional[float]:
        exp_str = market.get("expiration_time", "")
        if not exp_str:
            return None
        try:
            import datetime

            exp_dt = datetime.datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
            return (exp_dt.timestamp() - time.time()) / 3600.0
        except Exception:
            return None

    def _calculate_size(self) -> int:
        # Go heavy on "guaranteed" bets. Risk manager limits to max allocation.
        return 100 

    def get_stats(self) -> dict:
        return {
            "active_markets": self._active_markets,
            "opportunities_found": len(self._yields_captured),
        }
