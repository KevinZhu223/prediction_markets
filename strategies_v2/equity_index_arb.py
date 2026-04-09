"""
Equity Index Close Arbitrage Strategy

Exploits the latency between real-time equity index prices and
Kalshi's daily "S&P 500 Close" / "Nasdaq-100 Close" markets.

How it works:
  1. Kalshi has markets like "Will the S&P 500 close above 5,200 on Friday?"
     that settle at exactly 4:00 PM ET based on the official closing price.
  2. In the final 5-15 minutes of trading (3:45-4:00 PM), the actual
     index value becomes increasingly predictable.
  3. Kalshi's order books for these range contracts often lag the
     real-time futures/ETF prices by seconds.
  4. By monitoring SPY/QQQ prices via Yahoo Finance or a futures feed,
     we can determine early whether the index will "close above X" and
     sweep the mispriced contracts.

Target Kalshi series:
  - KXINXU      : S&P 500 daily close (above/below strike)  
  - KXNASDAQ100U: Nasdaq-100 daily close (above/below strike)

External feeds:
  - Yahoo Finance API (via yfinance or direct) for SPY / QQQ
  - Could also use Interactive Brokers or Alpaca for faster feeds
"""

import asyncio
import logging
import time
import re
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass

import httpx

from config import Config
from models import Signal, Side, ContractSide, Platform

logger = logging.getLogger(__name__)


# ── Index Ticker Mappings ──────────────────────────────────────────────────

INDEX_CONFIG = {
    "SPX": {
        "kalshi_series": "KXINXU",
        "etf_symbol": "SPY",       # SPDR S&P 500 ETF
        "multiplier": 10.0,         # SPY ≈ SPX / 10
        "yahoo_symbol": "^GSPC",    # Direct index symbol
    },
    "NDX": {
        "kalshi_series": "KXNASDAQ100U",
        "etf_symbol": "QQQ",
        "multiplier": 43.0,         # QQQ ≈ NDX / 43 (approximate)
        "yahoo_symbol": "^NDX",
    },
}


@dataclass
class IndexObservation:
    """Current index level from a real-time feed."""
    symbol: str           # "SPX" or "NDX"
    price: float          # Current index level
    change_pct: float     # Day's change percentage
    timestamp: float
    source: str           # "yahoo", "alpaca", etc.


class YahooFinanceFeed:
    """
    Lightweight Yahoo Finance price fetcher.
    Uses the v8 quote API for near-real-time index prices.
    """

    QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=5.0,
            headers={
                "User-Agent": "Mozilla/5.0 PredictionBot/1.0",
            },
        )

    async def get_price(self, symbol: str) -> Optional[IndexObservation]:
        """Fetch current price for a symbol (e.g. ^GSPC, ^NDX, SPY)."""
        try:
            url = self.QUOTE_URL.format(symbol=symbol)
            params = {"interval": "1m", "range": "1d"}

            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            result = data.get("chart", {}).get("result", [{}])[0]
            meta = result.get("meta", {})

            price = meta.get("regularMarketPrice", 0)
            prev_close = meta.get("chartPreviousClose", price)
            change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0

            return IndexObservation(
                symbol=symbol,
                price=price,
                change_pct=change_pct,
                timestamp=time.time(),
                source="yahoo",
            )

        except Exception as e:
            logger.error("Yahoo Finance fetch failed for %s: %s", symbol, e)
            return None

    async def close(self):
        await self._client.aclose()


class EquityIndexArbStrategy:
    """
    Monitors equity index levels and generates signals for Kalshi
    daily close markets during the final minutes of trading.
    """

    def __init__(self, kalshi_exchange=None):
        self.kalshi = kalshi_exchange
        self.feed = YahooFinanceFeed()
        self._active = False

        # Configuration
        self.sniper_window_minutes = 15     # Activate 15 min before close
        self.poll_interval_seconds = 10     # Poll every 10s during sniper mode
        self.idle_poll_seconds = 300        # Poll every 5 min when idle
        self.min_distance_pct = 0.10        # Min distance from strike (0.10%)
        self.min_edge_cents = 5             # Minimum edge to trade
        self._signal_cooldown_seconds = max(30.0, Config.EQUITY_SIGNAL_COOLDOWN_SECONDS)
        self._market_refresh_seconds = max(120.0, Config.EQUITY_MARKET_REFRESH_SECONDS)
        self._max_spread = max(0.0, Config.EQUITY_MAX_SPREAD)

        # Market close time (4:00 PM ET = 20:00 UTC during EDT)
        self.market_close_hour_utc = 20     # Adjust for DST
        self.market_close_minute = 0

        # Active markets
        self._active_markets: list[dict] = []
        self._signals: list[Signal] = []
        self._last_signal_by_market: dict[str, float] = {}
        self._last_market_refresh_ts = 0.0

    async def start(self):
        self._active = True
        logger.info("Equity index arb: started (sniper_window=%d min)", self.sniper_window_minutes)

    async def stop(self):
        self._active = False
        await self.feed.close()

    async def run_loop(self, signal_callback=None):
        """Main loop: monitor equity indices and generate signals."""
        while self._active:
            try:
                now = datetime.now(timezone.utc)
                minutes_to_close = self._minutes_to_market_close(now)

                if 0 < minutes_to_close <= self.sniper_window_minutes:
                    # SNIPER MODE — we're in the final minutes
                    logger.info(
                        "INDEX SNIPER: %.1f minutes to close, activating",
                        minutes_to_close,
                    )
                    await self._sniper_scan(minutes_to_close, signal_callback)
                    await asyncio.sleep(self.poll_interval_seconds)
                else:
                    # Idle mode — just monitor
                    await asyncio.sleep(self.idle_poll_seconds)

            except Exception as e:
                logger.error("Equity index loop error: %s", e)
                await asyncio.sleep(30)

    async def _sniper_scan(self, minutes_to_close: float, signal_callback=None):
        """
        During the sniper window, fetch index prices and evaluate markets.
        """
        now_ts = time.time()
        if (
            not self._active_markets
            or (now_ts - self._last_market_refresh_ts) >= self._market_refresh_seconds
        ):
            await self._discover_markets(force_refresh=True)

        for index_name, config in INDEX_CONFIG.items():
            # Fetch real-time index level
            obs = await self.feed.get_price(config["yahoo_symbol"])
            if not obs:
                continue

            # Evaluate each matching Kalshi market
            matching_markets = [
                m for m in self._active_markets
                if config["kalshi_series"] in m.get("ticker", "")
            ]

            for market in matching_markets:
                signal = self._evaluate_index_market(
                    index_name=index_name,
                    index_price=obs.price,
                    market=market,
                    minutes_to_close=minutes_to_close,
                )
                if signal:
                    self._last_signal_by_market[signal.market_id] = now_ts
                    self._signals.append(signal)
                    logger.info(
                        "INDEX SIGNAL: %s %s (reason: %s)",
                        signal.action.value, signal.market_id,
                        signal.reason
                    )
                    if signal_callback:
                        await signal_callback(signal)

    def _evaluate_index_market(
        self,
        index_name: str,
        index_price: float,
        market: dict,
        minutes_to_close: float,
    ) -> Optional[Signal]:
        """
        Compare current index level to the market's strike price.
        """
        ticker = market.get("ticker", "")
        title = market.get("title", "")
        
        # In live env, prefer _dollars fields
        yb_d = market.get("yes_bid_dollars")
        ya_d = market.get("yes_ask_dollars")
        
        if yb_d is not None:
            yes_bid = float(yb_d)
            yes_ask = float(ya_d) if ya_d is not None else 0.0
        else:
            yes_bid = (market.get("yes_bid", 0) or 0) / 100.0
            yes_ask = (market.get("yes_ask", 0) or 0) / 100.0

        if yes_ask <= 0 and yes_bid <= 0:
            return None

        last_signal_ts = self._last_signal_by_market.get(ticker, 0.0)
        if (time.time() - last_signal_ts) < self._signal_cooldown_seconds:
            return None

        if yes_bid > 0 and yes_ask > 0 and (yes_ask - yes_bid) > self._max_spread:
            return None

        volume = float(market.get("volume_fp", 0) or 0)
        if volume < 100:
            return None

        # Parse strike from title
        strike = self._parse_strike_from_title(title)
        if not strike:
            return None

        # Calculate distance
        distance = index_price - strike
        distance_pct = abs(distance / strike) * 100

        if distance_pct < self.min_distance_pct:
            return None  # Too close to call

        # Confidence increases with:
        #   1. Distance from strike (further = more certain)
        #   2. Less time to close (closer to settle = more certain)
        time_factor = max(0, 1 - (minutes_to_close / self.sniper_window_minutes))
        distance_factor = min(1.0, distance_pct / 1.0)  # 1% = max confidence
        confidence = min(0.95, 0.50 + time_factor * 0.25 + distance_factor * 0.20)

        if distance > 0:
            # Index is ABOVE strike → YES should be high
            fair_yes = min(0.95, 0.60 + distance_factor * 0.30 + time_factor * 0.10)
            if yes_ask < fair_yes - 0.05:
                edge = (fair_yes - yes_ask) * 100
                if edge >= self.min_edge_cents:
                    return Signal(
                        strategy="equity_index_arb",
                        action=Side.BUY,
                        contract_side=ContractSide.YES,
                        platform=Platform.KALSHI,
                        market_id=ticker,
                        target_price=yes_ask,
                        quantity=self._calculate_size(edge),
                        confidence=confidence,
                        reason=f"{index_name} at {index_price:.1f} > strike {strike:.1f} ({distance_pct:.2f}% above, {minutes_to_close:.0f}min left)",
                    )
        else:
            # Index is BELOW strike → NO should be high
            fair_no = min(0.95, 0.60 + distance_factor * 0.30 + time_factor * 0.10)
            no_ask = 1.0 - yes_bid
            if no_ask > 0 and no_ask < fair_no - 0.05:
                edge = (fair_no - no_ask) * 100
                if edge >= self.min_edge_cents:
                    return Signal(
                        strategy="equity_index_arb",
                        action=Side.BUY,
                        contract_side=ContractSide.NO,
                        platform=Platform.KALSHI,
                        market_id=ticker,
                        target_price=no_ask,
                        quantity=self._calculate_size(edge),
                        confidence=confidence,
                        reason=f"{index_name} at {index_price:.1f} < strike {strike:.1f} ({distance_pct:.2f}% below, {minutes_to_close:.0f}min left)",
                    )

        return None

    def _calculate_size(self, edge: float) -> int:
        return max(1, min(100, int(edge * 2)))

    async def _discover_markets(self, force_refresh: bool = False):
        """Find active index close markets on Kalshi."""
        if not self.kalshi:
            return

        now = time.time()
        if (
            not force_refresh
            and self._active_markets
            and (now - self._last_market_refresh_ts) < self._market_refresh_seconds
        ):
            return

        active_markets: dict[str, dict] = {}

        for config in INDEX_CONFIG.values():
            try:
                markets = await self.kalshi.fetch_markets(category=config["kalshi_series"])
                for market in markets:
                    if (market.get("status") or "").lower() not in {"active", "open"}:
                        continue
                    ticker = market.get("ticker", "")
                    if not ticker:
                        continue
                    active_markets[ticker] = market
            except Exception as e:
                logger.error("Index market discovery failed: %s", e)

        self._active_markets = list(active_markets.values())
        self._last_market_refresh_ts = now
        logger.info("Equity index: found %d active markets", len(self._active_markets))

    def _minutes_to_market_close(self, now: datetime) -> float:
        """Calculate minutes remaining until 4:00 PM ET market close."""
        close_today = now.replace(
            hour=self.market_close_hour_utc,
            minute=self.market_close_minute,
            second=0,
            microsecond=0,
        )
        delta = (close_today - now).total_seconds() / 60.0
        return delta

    @staticmethod
    def _parse_strike_from_title(title: str) -> Optional[float]:
        """
        Extract the strike price from a Kalshi market title.
        Examples:
          "S&P 500 above 5,200 on Friday?" → 5200.0
          "Nasdaq-100 above 18,500?"        → 18500.0
        """
        match = re.search(r'above\s+([\d,]+(?:\.\d+)?)', title, re.IGNORECASE)
        if match:
            return float(match.group(1).replace(",", ""))

        match = re.search(r'([\d,]+(?:\.\d+)?)\s*(?:or higher|or more)', title, re.IGNORECASE)
        if match:
            return float(match.group(1).replace(",", ""))

        return None

    def get_stats(self) -> dict:
        return {
            "active_markets": len(self._active_markets),
            "signals_generated": len(self._signals),
            "indices_tracked": list(INDEX_CONFIG.keys()),
        }
