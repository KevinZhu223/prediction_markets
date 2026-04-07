"""
Crypto Spot Price Feed
Monitors real-time BTC/ETH spot prices for latency arbitrage.
Uses Coinbase WebSocket (US-accessible) as primary feed,
with Kraken as fallback.
"""

import asyncio
import json
import logging
import time
from collections import deque
from typing import Callable, Awaitable, Optional

import websockets

from models import SpotPriceUpdate

logger = logging.getLogger(__name__)

# US-accessible WebSocket endpoints
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
KRAKEN_WS_URL = "wss://ws.kraken.com"


class CryptoFeed:
    """
    Real-time spot price monitor via Coinbase (or Kraken) WebSocket.
    Tracks price history in a rolling window and detects
    rapid moves that exceed a configurable threshold.
    """

    def __init__(
        self,
        symbols: list[str] = None,
        jump_threshold: float = 50.0,
        window_seconds: float = 5.0,
    ):
        # Normalize symbols to Coinbase format
        self._raw_symbols = symbols or ["btcusdt", "ethusdt"]
        self.jump_threshold = jump_threshold
        self.window_seconds = window_seconds

        # Symbol mapping: internal key -> Coinbase product ID
        self._symbol_map = {}
        self._reverse_map = {}
        for s in self._raw_symbols:
            s_lower = s.lower()
            if "btc" in s_lower:
                self._symbol_map[s_lower] = "BTC-USD"
                self._reverse_map["BTC-USD"] = s.upper()
            elif "eth" in s_lower:
                self._symbol_map[s_lower] = "ETH-USD"
                self._reverse_map["ETH-USD"] = s.upper()

        self._coinbase_products = list(set(self._symbol_map.values()))

        # Rolling price history per symbol: deque of (timestamp, price)
        self._price_history: dict[str, deque] = {
            s: deque(maxlen=500) for s in self._raw_symbols
        }
        self._latest_prices: dict[str, float] = {}
        self._callbacks: list[Callable[[SpotPriceUpdate, Optional[float]], Awaitable[None]]] = []
        self._running = False

    def on_update(self, callback: Callable[[SpotPriceUpdate, Optional[float]], Awaitable[None]]):
        """
        Register a callback for price updates.
        callback(update, price_delta_in_window) -- delta is None if no jump detected.
        """
        self._callbacks.append(callback)

    async def start(self):
        """Connect to Coinbase and stream trade data."""
        self._running = True
        logger.info("Crypto feed: connecting to Coinbase for %s", self._coinbase_products)

        while self._running:
            try:
                async for ws in websockets.connect(COINBASE_WS_URL, ping_interval=20):
                    try:
                        # Subscribe to ticker channel (trades + best bid/ask)
                        subscribe_msg = {
                            "type": "subscribe",
                            "product_ids": self._coinbase_products,
                            "channels": ["ticker"],
                        }
                        await ws.send(json.dumps(subscribe_msg))
                        logger.info("Crypto feed: subscribed to Coinbase ticker")

                        async for raw in ws:
                            if not self._running:
                                break
                            await self._handle_coinbase_message(raw)
                    except websockets.ConnectionClosed:
                        logger.warning("Coinbase WS disconnected, reconnecting in 2s...")
                        await asyncio.sleep(2)
            except Exception as e:
                logger.error("Crypto feed error: %s", e)
                # Try Kraken as fallback
                logger.info("Attempting Kraken fallback...")
                try:
                    await self._run_kraken_feed()
                except Exception as e2:
                    logger.error("Kraken fallback also failed: %s", e2)
                    await asyncio.sleep(5)

    async def stop(self):
        self._running = False

    async def _handle_coinbase_message(self, raw: str):
        """Parse Coinbase ticker messages."""
        try:
            msg = json.loads(raw)
            if msg.get("type") != "ticker":
                return

            product_id = msg.get("product_id", "")
            price = float(msg.get("price", 0))
            ts = time.time()

            # Map back to internal symbol
            internal_symbol = self._reverse_map.get(product_id)
            if not internal_symbol:
                return

            internal_key = internal_symbol.lower()

            # Update history
            if internal_key in self._price_history:
                self._price_history[internal_key].append((ts, price))
            self._latest_prices[internal_key] = price

            # Detect rapid movement
            delta = self._detect_jump(internal_key, ts)

            update = SpotPriceUpdate(symbol=internal_symbol, price=price, timestamp=ts)

            for cb in self._callbacks:
                try:
                    await cb(update, delta)
                except Exception as e:
                    logger.error("Callback error: %s", e)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug("Coinbase message parse error: %s", e)

    async def _run_kraken_feed(self):
        """Fallback: use Kraken WebSocket."""
        kraken_pairs = []
        kraken_map = {}
        for s in self._raw_symbols:
            s_lower = s.lower()
            if "btc" in s_lower:
                kraken_pairs.append("XBT/USD")
                kraken_map["XBT/USD"] = s.upper()
            elif "eth" in s_lower:
                kraken_pairs.append("ETH/USD")
                kraken_map["ETH/USD"] = s.upper()

        async for ws in websockets.connect(KRAKEN_WS_URL, ping_interval=20):
            subscribe_msg = {
                "event": "subscribe",
                "pair": kraken_pairs,
                "subscription": {"name": "trade"},
            }
            await ws.send(json.dumps(subscribe_msg))
            logger.info("Crypto feed: subscribed to Kraken trade feed")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    if isinstance(msg, list) and len(msg) >= 4:
                        trades = msg[1]
                        pair = msg[3]
                        internal_symbol = kraken_map.get(pair)
                        if internal_symbol and trades:
                            last_trade = trades[-1]
                            price = float(last_trade[0])
                            ts = time.time()
                            internal_key = internal_symbol.lower()

                            if internal_key in self._price_history:
                                self._price_history[internal_key].append((ts, price))
                            self._latest_prices[internal_key] = price

                            delta = self._detect_jump(internal_key, ts)
                            update = SpotPriceUpdate(symbol=internal_symbol, price=price, timestamp=ts)
                            for cb in self._callbacks:
                                await cb(update, delta)
                except Exception:
                    pass

    def _detect_jump(self, symbol: str, now: float) -> Optional[float]:
        """
        Check if the price moved by >= jump_threshold in the last window_seconds.
        Returns the signed delta if threshold exceeded, else None.
        """
        history = self._price_history.get(symbol)
        if not history or len(history) < 2:
            return None

        cutoff = now - self.window_seconds
        oldest_price = None
        for ts, p in history:
            if ts >= cutoff:
                oldest_price = p
                break

        if oldest_price is None:
            return None

        current_price = history[-1][1]
        delta = current_price - oldest_price

        if abs(delta) >= self.jump_threshold:
            logger.info(
                "PRICE JUMP: %s $%.2f -> $%.2f (delta $%.2f in %.1fs)",
                symbol.upper(), oldest_price, current_price, delta, self.window_seconds,
            )
            return delta

        return None

    def get_latest_price(self, symbol: str) -> Optional[float]:
        return self._latest_prices.get(symbol.lower())
