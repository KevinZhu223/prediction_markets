"""
Forex Latency Arbitrage Strategy

Exploits the lag between real-time forex spot prices and Kalshi's
hourly forex directional markets.

How it works:
  1. Kalshi has hourly markets like "Will EUR/USD be above 1.1350 at 2 PM ET?"
     that settle based on the official FX fixing at the top of each hour.
  2. Yahoo Finance provides free, near-real-time forex quotes via the
     EURUSD=X and JPY=X symbols with ~1-2 second latency.
  3. If the current EUR/USD spot is 1.1380 and the Kalshi YES contract for
     "above 1.1350" is priced at $0.70, there's edge because the observed
     spot already implies ~85% probability (time-decay adjusted).

Target Kalshi series:
  - KXEURUSD  : Hourly EUR/USD directional
  - KXUSDJPY  : Hourly USD/JPY directional

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


# ── Forex Pair Configuration ──────────────────────────────────────────────

FOREX_PAIRS = {
    "EURUSD": {
        "yahoo_symbol": "EURUSD=X",
        "kalshi_series": ["KXEURUSD"],
        "typical_spread_pips": 1.0,   # ~0.0001
        "pip_size": 0.0001,
    },
    "USDJPY": {
        "yahoo_symbol": "JPY=X",
        "kalshi_series": ["KXUSDJPY"],
        "typical_spread_pips": 1.5,
        "pip_size": 0.01,
    },
}


class YahooForexFeed:
    """
    Lightweight Yahoo Finance forex price fetcher.
    Uses the v8 chart API for near-real-time forex quotes.
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

    def _log_error_throttled(self, key: str, message: str, interval_seconds: float = 60.0):
        now = time.time()
        last = self._last_error_log_ts.get(key, 0.0)
        if (now - last) >= interval_seconds:
            logger.error(message)
            self._last_error_log_ts[key] = now
        else:
            logger.debug(message)

    async def get_price(self, symbol: str) -> Optional[float]:
        """Fetch current price for a forex symbol (e.g. EURUSD=X)."""
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
                return float(price)
            return None

        except Exception as e:
            self._log_error_throttled(
                f"yahoo:{symbol}",
                f"Yahoo Finance forex fetch failed for {symbol}: {e}",
            )
            return None

    async def close(self):
        await self._client.aclose()


class ForexArbStrategy:
    """
    Monitors forex spot prices and generates signals for Kalshi
    hourly forex markets.

    Uses the same time-decay (theta) model as the crypto latency arb:
    signals generated close to the hourly settlement are much more
    reliable than early-hour signals.
    """

    def __init__(self, kalshi_exchange=None):
        self.kalshi = kalshi_exchange
        self.feed = YahooForexFeed()
        self._active = False

        # Configuration from config.py
        self._poll_interval_seconds = max(10.0, Config.FOREX_POLL_INTERVAL_SECONDS)
        self._min_edge = max(0.0, Config.FOREX_MIN_EDGE)
        self._max_spread = max(0.0, Config.FOREX_MAX_SPREAD)
        self._max_hours_to_expiry = max(0.25, Config.FOREX_MAX_HOURS_TO_EXPIRY)
        self._signal_cooldown_seconds = max(30.0, Config.FOREX_SIGNAL_COOLDOWN_SECONDS)

        # State
        self._active_markets: list[dict] = []
        self._signals: list[Signal] = []
        self._last_signal_by_market: dict[str, float] = {}
        self._last_discovery_ts = 0.0
        self._discovery_interval_seconds = 300.0  # Refresh markets every 5 min

    async def start(self):
        self._active = True
        logger.info(
            "Forex arb: started (poll=%.0fs, max_expiry=%.1fh)",
            self._poll_interval_seconds,
            self._max_hours_to_expiry,
        )

    async def stop(self):
        self._active = False
        await self.feed.close()

    async def run_loop(self, signal_callback=None):
        """Main loop: discover forex markets, poll Yahoo, generate signals."""
        while self._active:
            try:
                # Step 1: Discover active forex markets on Kalshi
                now = time.time()
                if (
                    not self._active_markets
                    or (now - self._last_discovery_ts) >= self._discovery_interval_seconds
                ):
                    await self._discover_markets()
                    self._last_discovery_ts = now

                # Step 2: For each forex pair, fetch spot and evaluate markets
                for pair_name, pair_config in FOREX_PAIRS.items():
                    spot = await self.feed.get_price(pair_config["yahoo_symbol"])
                    if not spot:
                        continue

                    # Find matching Kalshi markets for this pair
                    matching = [
                        m for m in self._active_markets
                        if any(
                            series in m.get("ticker", "")
                            for series in pair_config["kalshi_series"]
                        )
                    ]

                    for market in matching:
                        signal = self._evaluate_market(
                            pair_name=pair_name,
                            spot_price=spot,
                            market=market,
                            pip_size=pair_config["pip_size"],
                        )
                        if signal:
                            self._signals.append(signal)
                            logger.info(
                                "FOREX SIGNAL: %s %s (reason: %s)",
                                signal.action.value, signal.market_id,
                                signal.reason,
                            )
                            if signal_callback:
                                await signal_callback(signal)

            except Exception as e:
                logger.error("Forex arb loop error: %s", e)

            await asyncio.sleep(self._poll_interval_seconds)

    async def _discover_markets(self):
        """Find active hourly forex markets on Kalshi."""
        if not self.kalshi:
            return

        try:
            all_markets = await self.kalshi.fetch_markets()

            active_markets: list[dict] = []
            seen_tickers: set[str] = set()

            for m in all_markets:
                if m.get("status") != "active":
                    continue
                ticker = m.get("ticker", "")
                title = m.get("title", "").lower()

                # Match forex-related markets
                is_forex = (
                    "eur/usd" in title
                    or "usd/jpy" in title
                    or "eurusd" in ticker.upper()
                    or "usdjpy" in ticker.upper()
                    or "forex" in title
                    or "exchange rate" in title
                )
                if not is_forex:
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

            self._active_markets = active_markets[:30]
            logger.info(
                "Forex arb: tracking %d active forex markets",
                len(self._active_markets),
            )

        except Exception as e:
            logger.error("Forex market discovery failed: %s", e)

    def _evaluate_market(
        self,
        pair_name: str,
        spot_price: float,
        market: dict,
        pip_size: float,
    ) -> Optional[Signal]:
        """
        Compare current forex spot to the market's strike price.
        Uses the same time-decay model as the crypto latency arb.
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

        # Hours to expiry
        hours_to_expiry = self._hours_to_expiry(market)
        if hours_to_expiry is None or hours_to_expiry <= 0:
            return None

        # Parse strike from market data or title
        strike = self._parse_strike(market, title, pair_name)
        if not strike or strike <= 0:
            return None

        # Calculate distance in pips
        distance = spot_price - strike
        distance_pips = abs(distance) / pip_size

        # Need at least 5 pips of distance to have a meaningful signal
        if distance_pips < 5:
            return None

        # Time-decay multiplier (same logic as crypto latency arb)
        minutes_to_expiry = hours_to_expiry * 60.0
        if minutes_to_expiry <= 5.0:
            time_multiplier = 1.0
        elif minutes_to_expiry <= 15.0:
            time_multiplier = 0.85
        elif minutes_to_expiry <= 30.0:
            time_multiplier = 0.65
        else:
            time_multiplier = 0.45

        # Fair value based on pip distance and time
        pip_factor = min(1.0, distance_pips / 50.0)  # 50 pips = max confidence
        base_fair = 0.55 + pip_factor * 0.40  # 0.55 to 0.95
        adjusted_fair = 0.50 + (base_fair - 0.50) * time_multiplier

        confidence = min(0.95, adjusted_fair)

        if distance > 0:
            # Spot ABOVE strike → YES is likely
            edge = adjusted_fair - yes_ask
            if edge >= self._min_edge:
                self._last_signal_by_market[ticker] = time.time()
                return Signal(
                    strategy="forex_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=yes_ask,
                    quantity=self._calculate_size(edge * 100),
                    confidence=confidence,
                    reason=(
                        f"{pair_name} spot={spot_price:.4f} > strike={strike:.4f} "
                        f"({distance_pips:.0f} pips, fair={adjusted_fair:.2f}, "
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
                    strategy="forex_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.NO,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=no_ask,
                    quantity=self._calculate_size(edge * 100),
                    confidence=confidence,
                    reason=(
                        f"{pair_name} spot={spot_price:.4f} < strike={strike:.4f} "
                        f"({distance_pips:.0f} pips, fair={adjusted_fair:.2f}, "
                        f"{minutes_to_expiry:.0f}min left)"
                    ),
                )

        return None

    def _calculate_size(self, edge_cents: float) -> int:
        return max(1, min(100, int(edge_cents * 2)))

    def _parse_strike(self, market: dict, title: str, pair_name: str) -> Optional[float]:
        """Extract the strike price from market data or title."""
        # 1. Prefer floor_strike from market data
        fs = market.get("floor_strike")
        if fs is not None:
            try:
                return float(fs)
            except (TypeError, ValueError):
                pass

        # 2. Try regex on title: "above 1.1350" or "above 148.50"
        match = re.search(r'above\s+([\d.]+)', title, re.IGNORECASE)
        if match:
            return float(match.group(1))

        # 3. Try regex on ticker suffix
        match = re.search(r'[-_]([\d.]+)$', market.get("ticker", ""))
        if match:
            return float(match.group(1))

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
            "pairs_tracked": list(FOREX_PAIRS.keys()),
        }
