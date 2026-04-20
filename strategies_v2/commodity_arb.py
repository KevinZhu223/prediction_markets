"""
Commodity Price Arbitrage Strategy

Exploits the lag between real-time commodity futures prices and Kalshi's
hourly/daily commodity price markets (WTI Crude, Gold, Natural Gas, etc.).

How it works:
  1. Kalshi has markets like "Will WTI crude close above $62.99 today?"
     (KXWTI series) and similar for Gold, Natural Gas, etc.
  2. Yahoo Finance provides free, near-real-time commodity quotes via
     symbols like CL=F (WTI Crude), GC=F (Gold), NG=F (Natural Gas).
  3. If current WTI is $65.50 and the Kalshi YES contract for
     "above $62.99" is priced at $0.70, there's edge because the observed
     spot already implies much higher probability.

Target Kalshi series:
  - KXWTI        : WTI Crude Oil daily/hourly directional
  - KXGOLD       : Gold price directional
  - KXNATGAS     : Natural Gas directional
  - KXLITHIUMW   : Lithium weekly price (COMEX)

Data source:
  - Yahoo Finance API (free, no API key required)
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from config import Config
from models import Signal, Side, ContractSide, Platform

logger = logging.getLogger(__name__)


# ── Commodity Configuration ───────────────────────────────────────────────

COMMODITY_PAIRS = {
    "WTI": {
        "yahoo_symbol": "CL=F",
        "kalshi_prefixes": ["KXWTI"],
        "tick_size": 0.01,          # $ per tick
        "min_distance_pct": 0.5,    # 0.5% minimum distance to trade
    },
    "GOLD": {
        "yahoo_symbol": "GC=F",
        "kalshi_prefixes": ["KXGOLD"],
        "tick_size": 0.10,
        "min_distance_pct": 0.3,
    },
    "NATGAS": {
        "yahoo_symbol": "NG=F",
        "kalshi_prefixes": ["KXNATGAS"],
        "tick_size": 0.001,
        "min_distance_pct": 1.0,    # Nat gas is volatile, need bigger margin
    },
    # LITHIUM removed — 0/3 win rate in settlements, ALI=F is a poor proxy
    # for KXLITHIUMW which tracks a different LME contract. Skip until better data.
}


class YahooCommodityFeed:
    """
    Lightweight Yahoo Finance commodity price fetcher.
    Uses the v8 chart API for near-real-time futures quotes.
    """

    QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=5.0,
            headers={
                "User-Agent": "Mozilla/5.0 PredictionBot/1.0",
            },
        )
        self._last_error_log_ts: dict[str, float] = {}
        self._price_cache: dict[str, tuple[float, float]] = {}  # symbol -> (price, timestamp)
        self._cache_ttl = 15.0  # Cache prices for 15 seconds

    def _log_error_throttled(self, key: str, message: str, interval_seconds: float = 120.0):
        now = time.time()
        last = self._last_error_log_ts.get(key, 0.0)
        if (now - last) >= interval_seconds:
            logger.error(message)
            self._last_error_log_ts[key] = now
        else:
            logger.debug(message)

    async def get_price(self, symbol: str) -> Optional[float]:
        """Fetch current price for a commodity futures symbol."""
        # Check cache
        cached = self._price_cache.get(symbol)
        if cached and (time.time() - cached[1]) < self._cache_ttl:
            return cached[0]

        try:
            url = self.QUOTE_URL.format(symbol=symbol)
            params = {"interval": "1m", "range": "1d"}

            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            result = data.get("chart", {}).get("result", [{}])[0]
            meta = result.get("meta", {})
            price = meta.get("regularMarketPrice", 0)

            if price and price > 0:
                self._price_cache[symbol] = (float(price), time.time())
                return float(price)
            return None

        except Exception as e:
            self._log_error_throttled(
                f"yahoo:{symbol}",
                f"Yahoo Finance commodity fetch failed for {symbol}: {e}",
            )
            return None

    async def close(self):
        await self._client.aclose()


class CommodityArbStrategy:
    """
    Monitors commodity futures prices and generates signals for Kalshi
    commodity price markets.

    Uses time-decay (theta) model: signals generated close to settlement
    are much more reliable than early signals.
    """

    def __init__(self, kalshi_exchange=None):
        self.kalshi = kalshi_exchange
        self.feed = YahooCommodityFeed()
        self._active = False

        # Configuration
        self._poll_interval_seconds = 45.0
        self._min_edge = 0.06              # 6% minimum edge
        self._max_spread = 0.12
        self._max_hours_to_expiry = 4.0    # Trade up to 4h before expiry
        self._signal_cooldown_seconds = 300.0

        # State
        self._active_markets: list[dict] = []
        self._signals: list[Signal] = []
        self._last_signal_by_market: dict[str, float] = {}
        self._last_discovery_ts = 0.0
        self._discovery_interval_seconds = 300.0

    async def start(self):
        self._active = True
        logger.info(
            "Commodity arb: started (poll=%.0fs, max_expiry=%.1fh)",
            self._poll_interval_seconds,
            self._max_hours_to_expiry,
        )

    async def stop(self):
        self._active = False
        await self.feed.close()

    async def run_loop(self, signal_callback=None):
        """Main loop: discover commodity markets, poll Yahoo, generate signals."""
        while self._active:
            try:
                now = time.time()
                if (
                    not self._active_markets
                    or (now - self._last_discovery_ts) >= self._discovery_interval_seconds
                ):
                    await self._discover_markets()
                    self._last_discovery_ts = now

                for commodity_name, commodity_config in COMMODITY_PAIRS.items():
                    spot = await self.feed.get_price(commodity_config["yahoo_symbol"])
                    if not spot:
                        continue

                    matching = [
                        m for m in self._active_markets
                        if any(
                            prefix in m.get("ticker", "")
                            for prefix in commodity_config["kalshi_prefixes"]
                        )
                    ]

                    for market in matching:
                        signal = self._evaluate_market(
                            commodity_name=commodity_name,
                            spot_price=spot,
                            market=market,
                            min_distance_pct=commodity_config["min_distance_pct"],
                        )
                        if signal:
                            self._signals.append(signal)
                            logger.info(
                                "COMMODITY SIGNAL: %s %s (reason: %s)",
                                signal.action.value, signal.market_id,
                                signal.reason,
                            )
                            if signal_callback:
                                await signal_callback(signal)

                    # small delay between commodities
                    await asyncio.sleep(0.5)

            except Exception as e:
                logger.error("Commodity arb loop error: %s", e)

            await asyncio.sleep(self._poll_interval_seconds)

    async def _discover_markets(self):
        """Find active commodity price markets on Kalshi."""
        if not self.kalshi:
            return

        try:
            all_markets = await self.kalshi.fetch_markets()

            active_markets: list[dict] = []
            seen_tickers: set[str] = set()

            # Collect all known prefixes
            all_prefixes = []
            for config in COMMODITY_PAIRS.values():
                all_prefixes.extend(config["kalshi_prefixes"])

            for m in all_markets:
                if m.get("status") != "active":
                    continue
                ticker = m.get("ticker", "")
                title = m.get("title", "").lower()

                # Match commodity-related markets by prefix or title keywords
                is_commodity = (
                    any(ticker.startswith(prefix) for prefix in all_prefixes)
                    or "crude" in title
                    or "wti" in title
                    or "oil price" in title
                    or "gold price" in title
                    or "natural gas" in title
                    or "lithium" in title
                )
                if not is_commodity:
                    continue

                if not ticker or ticker in seen_tickers:
                    continue

                hours_to_expiry = self._hours_to_expiry(m)
                if hours_to_expiry is None or hours_to_expiry <= 0:
                    continue
                if hours_to_expiry > self._max_hours_to_expiry:
                    continue

                seen_tickers.add(ticker)
                active_markets.append(m)

            active_markets.sort(
                key=lambda x: (
                    self._hours_to_expiry(x) or 9999.0,
                    -float(x.get("volume_fp", 0) or 0),
                )
            )

            self._active_markets = active_markets[:40]
            logger.info(
                "Commodity arb: tracking %d active commodity markets",
                len(self._active_markets),
            )

        except Exception as e:
            logger.error("Commodity market discovery failed: %s", e)

    def _evaluate_market(
        self,
        commodity_name: str,
        spot_price: float,
        market: dict,
        min_distance_pct: float,
    ) -> Optional[Signal]:
        """
        Compare current commodity spot to the market's strike price.
        """
        ticker = market.get("ticker", "")
        title = market.get("title", "")

        # Check cooldown
        last_signal_ts = self._last_signal_by_market.get(ticker, 0.0)
        if (time.time() - last_signal_ts) < self._signal_cooldown_seconds:
            return None

        # Get prices
        yes_bid_d = market.get("yes_bid_dollars")
        yes_ask_d = market.get("yes_ask_dollars")
        yes_bid = float(yes_bid_d) if yes_bid_d and float(yes_bid_d) > 0 else 0.0
        yes_ask = float(yes_ask_d) if yes_ask_d and float(yes_ask_d) > 0 else 0.0

        if yes_ask <= 0:
            return None

        # Spread filter
        if yes_bid > 0 and (yes_ask - yes_bid) > self._max_spread:
            return None

        hours_to_expiry = self._hours_to_expiry(market)
        if hours_to_expiry is None or hours_to_expiry <= 0:
            return None

        # Parse strike
        strike = self._parse_strike(market, title)
        if not strike or strike <= 0:
            return None

        # Calculate distance
        distance = spot_price - strike
        distance_pct = abs(distance) / strike * 100

        if distance_pct < min_distance_pct:
            return None

        # Time-decay multiplier
        minutes_to_expiry = hours_to_expiry * 60.0
        if minutes_to_expiry <= 10.0:
            time_multiplier = 1.0
        elif minutes_to_expiry <= 30.0:
            time_multiplier = 0.85
        elif minutes_to_expiry <= 60.0:
            time_multiplier = 0.70
        elif minutes_to_expiry <= 120.0:
            time_multiplier = 0.55
        else:
            time_multiplier = 0.40

        # Fair value based on distance and time
        dist_factor = min(1.0, distance_pct / 5.0)  # 5% distance = max confidence
        base_fair = 0.55 + dist_factor * 0.40  # 0.55 to 0.95
        adjusted_fair = 0.50 + (base_fair - 0.50) * time_multiplier

        confidence = min(0.95, adjusted_fair)

        if distance > 0:
            # Spot ABOVE strike → YES is likely
            edge = adjusted_fair - yes_ask
            if edge >= self._min_edge:
                self._last_signal_by_market[ticker] = time.time()
                return Signal(
                    strategy="commodity_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=yes_ask,
                    quantity=self._calculate_size(edge * 100),
                    confidence=confidence,
                    reason=(
                        f"{commodity_name} spot=${spot_price:.2f} > strike=${strike:.2f} "
                        f"({distance_pct:.1f}% above, fair={adjusted_fair:.2f}, "
                        f"{minutes_to_expiry:.0f}min left)"
                    ),
                )

        else:
            # Spot BELOW strike → NO is likely
            no_ask = 1.0 - yes_bid
            if no_ask <= 0:
                return None
            edge = adjusted_fair - no_ask
            if edge >= self._min_edge:
                self._last_signal_by_market[ticker] = time.time()
                return Signal(
                    strategy="commodity_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.NO,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=no_ask,
                    quantity=self._calculate_size(edge * 100),
                    confidence=confidence,
                    reason=(
                        f"{commodity_name} spot=${spot_price:.2f} < strike=${strike:.2f} "
                        f"({distance_pct:.1f}% below, fair={adjusted_fair:.2f}, "
                        f"{minutes_to_expiry:.0f}min left)"
                    ),
                )

        return None

    def _calculate_size(self, edge_cents: float) -> int:
        return max(1, min(100, int(edge_cents * 2)))

    def _parse_strike(self, market: dict, title: str) -> Optional[float]:
        """Extract the strike price from market data or title."""
        # 1. Prefer floor_strike from market data
        fs = market.get("floor_strike")
        if fs is not None:
            try:
                return float(fs)
            except (TypeError, ValueError):
                pass

        # 2. Try regex on ticker suffix (e.g. "-T62.99", "-T158999.99")
        ticker = market.get("ticker", "")
        match = re.search(r'-T([\d.]+)$', ticker)
        if match:
            return float(match.group(1))

        # 3. Try regex on title: "above 62.99" or "above $62.99"
        match = re.search(r'above\s+\$?([\d,.]+)', title, re.IGNORECASE)
        if match:
            return float(match.group(1).replace(",", ""))

        return None

    @staticmethod
    def _hours_to_expiry(market: dict) -> Optional[float]:
        exp_str = market.get("expiration_time", "")
        if not exp_str:
            return None
        try:
            exp_dt = datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
            return (exp_dt.timestamp() - time.time()) / 3600.0
        except Exception:
            return None

    def get_stats(self) -> dict:
        return {
            "active_markets": len(self._active_markets),
            "signals_generated": len(self._signals),
            "commodities_tracked": list(COMMODITY_PAIRS.keys()),
        }
