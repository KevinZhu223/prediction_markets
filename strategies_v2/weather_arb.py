"""
Hourly Weather Arbitrage Strategy

Exploits the lag between real-time weather station observations and
Kalshi's hourly temperature markets.

How it works:
  1. Kalshi settles "Will the temp in NYC be above 75.99° at 1 AM EDT?"
     based on AccuWeather METAR data for station KNYC (Central Park).
  2. NOAA / AccuWeather METAR observations update every ~5-10 minutes.
  3. If the observation shows 78°F at 12:50 AM, and the Kalshi YES
     contract at strike 75.99 is still at 85¢, there's edge because
     the outcome is essentially decided.

Target Kalshi series:
  - KXTEMPNYCH : Hourly directional NYC temperature
  - KXHIGHNYD  : Hourly directional NYC temperature (alternate series)

Data sources:
  - NOAA api.weather.gov  (free, no API key, METAR observations)
  - AccuWeather portal (Kalshi's official settlement source)

IMPORTANT: Kalshi uses station KNYC (Central Park, coords 40.7812,-73.9665)
           for NYC markets — NOT KJFK (JFK Airport).
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass

import httpx

from config import Config

logger = logging.getLogger(__name__)


# ── NOAA Station Mappings ───────────────────────────────────────────────────

# Maps Kalshi location codes to NOAA/METAR station IDs.
# CRITICAL: These MUST match the stations Kalshi actually uses for settlement.
# Verified via Kalshi API: series.settlement_sources[].url contains station=KXXX.
#
# As of 2026-04: Only NYC hourly temp is actively listed on Kalshi.
# The station is KNYC (Central Park, Belvedere Castle — NOT KJFK/JFK Airport).
STATION_MAP = {
    "nyc":     "KNYC",   # Central Park, New York (Kalshi verified)
    "new york": "KNYC",  # Alternate name match
    "central park": "KNYC",
    "chicago": "KORD",   # O'Hare Airport, Chicago
    "la":      "KLAX",   # LAX Airport, Los Angeles
    "miami":   "KMIA",   # Miami International
    "dallas":  "KDFW",   # Dallas/Fort Worth
    "denver":  "KDEN",   # Denver International
    "seattle": "KSEA",   # Seattle-Tacoma
    "atlanta": "KATL",   # Hartsfield-Jackson Atlanta
}


@dataclass
class WeatherObservation:
    """A single NOAA weather station observation."""
    station_id: str
    timestamp: datetime
    temperature_f: Optional[float]
    temperature_c: Optional[float]
    precipitation_last_hour_mm: Optional[float]
    wind_speed_mph: Optional[float]
    humidity_pct: Optional[float]
    raw_text: str = ""

from models import Signal, Side, ContractSide, Platform


class NOAAClient:
    """
    Fetches real-time weather observations from the National Weather Service API.
    Documentation: https://www.weather.gov/documentation/services-web-api
    """

    BASE_URL = "https://api.weather.gov"

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "User-Agent": "(prediction-market-bot, contact@example.com)",
                "Accept": "application/geo+json",
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

    async def get_latest_observation(self, station_id: str) -> Optional[WeatherObservation]:
        """
        Fetch the most recent observation from a NOAA station.
        
        Example: get_latest_observation("KJFK") returns the latest
        temperature, precipitation, etc. from JFK airport.
        """
        url = f"{self.BASE_URL}/stations/{station_id}/observations/latest"
        
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            data = resp.json()

            props = data.get("properties", {})

            # Extract temperature (NOAA returns Celsius)
            temp_c = self._extract_value(props, "temperature")
            temp_f = (temp_c * 9 / 5 + 32) if temp_c is not None else None

            # Extract precipitation
            precip_mm = self._extract_value(props, "precipitationLastHour")

            # Extract wind
            wind_ms = self._extract_value(props, "windSpeed")
            wind_mph = (wind_ms * 2.237) if wind_ms is not None else None

            # Extract humidity
            humidity = self._extract_value(props, "relativeHumidity")

            # Parse timestamp
            ts_str = props.get("timestamp", "")
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                ts = datetime.now(timezone.utc)

            return WeatherObservation(
                station_id=station_id,
                timestamp=ts,
                temperature_f=temp_f,
                temperature_c=temp_c,
                precipitation_last_hour_mm=precip_mm,
                wind_speed_mph=wind_mph,
                humidity_pct=humidity,
                raw_text=props.get("rawMessage", ""),
            )

        except httpx.HTTPStatusError as e:
            self._log_error_throttled(
                f"http:{station_id}",
                f"NOAA API error for {station_id}: {e}",
            )
            return None
        except Exception as e:
            self._log_error_throttled(
                f"fetch:{station_id}",
                f"NOAA fetch failed for {station_id}: {e}",
            )
            return None

    async def get_recent_observations(
        self, station_id: str, limit: int = 6
    ) -> list[WeatherObservation]:
        """
        Fetch the last N observations to detect trends.
        Useful for predicting whether temperature will cross a threshold.
        """
        url = f"{self.BASE_URL}/stations/{station_id}/observations"
        params = {"limit": limit}

        try:
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            observations = []
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                temp_c = self._extract_value(props, "temperature")
                temp_f = (temp_c * 9 / 5 + 32) if temp_c is not None else None

                ts_str = props.get("timestamp", "")
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except Exception:
                    ts = datetime.now(timezone.utc)

                observations.append(WeatherObservation(
                    station_id=station_id,
                    timestamp=ts,
                    temperature_f=temp_f,
                    temperature_c=temp_c,
                    precipitation_last_hour_mm=self._extract_value(props, "precipitationLastHour"),
                    wind_speed_mph=None,
                    humidity_pct=None,
                ))

            return observations

        except Exception as e:
            logger.error("NOAA recent observations failed for %s: %s", station_id, e)
            return []

    @staticmethod
    def _extract_value(props: dict, key: str) -> Optional[float]:
        """Extract a numeric value from NOAA's nested JSON structure."""
        entry = props.get(key, {})
        if isinstance(entry, dict):
            val = entry.get("value")
            return float(val) if val is not None else None
        return None

    async def close(self):
        await self._client.aclose()


class WeatherArbStrategy:
    """
    Monitors NOAA stations and generates signals for Kalshi weather markets.
    
    Strategy logic:
    - For HOURLY TEMPERATURE markets ("above X°F at 1 AM"):
      - If observed temp > strike by margin → BUY YES (high confidence)
      - If observed temp < strike by margin → BUY NO (high confidence)
      
    - For DAILY HIGH TEMPERATURE markets ("daily high above X°F"):
      - NEVER buy NO in the morning/afternoon (temp hasn't peaked yet!)
      - Only buy YES if current temp already exceeds strike
      - Only buy NO after ~3 PM ET when the daily high is locked in
      
    - For PRECIPITATION markets ("will it rain?"):
      - If precip > 0.01mm observed → BUY YES
      - If precip == 0 and < 15 min to settle → BUY NO (risky but edgy)
    """

    def __init__(self, kalshi_exchange=None):
        self.noaa = NOAAClient()
        self.kalshi = kalshi_exchange
        self._active = False

        # Configuration
        self.poll_interval_seconds = 120  # Check every 2 minutes
        self.temp_confidence_margin = 4.0  # °F margin for high confidence (ULTRA CONSERVATIVE)
        self.min_edge_cents = 10  # Minimum edge to trade (10¢)
        self._observation_max_age_seconds = max(60.0, Config.WEATHER_OBSERVATION_MAX_AGE_SECONDS)
        self._obs_cache_ttl_seconds = max(5.0, Config.WEATHER_OBS_CACHE_TTL_SECONDS)
        self._max_markets_per_scan = max(10, Config.WEATHER_MAX_MARKETS_PER_SCAN)
        self._discovery_log_cooldown_seconds = max(
            10.0,
            Config.WEATHER_DISCOVERY_LOG_COOLDOWN_SECONDS,
        )
        self._signal_cooldown_seconds = max(30.0, Config.WEATHER_SIGNAL_COOLDOWN_SECONDS)
        self._max_spread = max(0.0, Config.WEATHER_MAX_SPREAD)
        self._max_hours_to_expiry = max(1.0, Config.WEATHER_MAX_HOURS_TO_EXPIRY)
        self._last_signal_by_market: dict[str, float] = {}
        self._station_obs_cache: dict[str, WeatherObservation] = {}
        self._station_obs_fetched_at: dict[str, float] = {}
        self._last_discovery_count = -1
        self._last_discovery_log_ts = 0.0

        # Track active markets
        self._active_markets: list[dict] = []
        self._signals_generated: list[Signal] = []

    async def start(self):
        self._active = True
        logger.info("Weather arb: started (poll_interval=%ds)", self.poll_interval_seconds)

    async def stop(self):
        self._active = False
        await self.noaa.close()

    async def run_loop(self, signal_callback=None):
        """Main loop: discover weather markets, poll NOAA, generate signals."""
        while self._active:
            try:
                # Step 1: Discover active weather markets on Kalshi
                await self._discover_markets()

                # Step 2: For each market, check NOAA and evaluate
                for market in self._active_markets:
                    signal = await self._evaluate_market(market)
                    if signal:
                        self._signals_generated.append(signal)
                        logger.info(
                            "WEATHER SIGNAL: %s %s (reason: %s)",
                            signal.action, signal.market_id,
                            signal.reason,
                        )
                        if signal_callback:
                            await signal_callback(signal)

            except Exception as e:
                logger.error("Weather arb loop error: %s", e)

            await asyncio.sleep(self.poll_interval_seconds)

    async def _discover_markets(self):
        """Find active hourly and daily-high weather markets on Kalshi."""
        if not self.kalshi:
            return

        try:
            # Broaden discovery: Fetch all and filter by keywords in title
            all_markets = await self.kalshi.fetch_markets()

            active_markets: list[dict] = []
            seen_tickers: set[str] = set()
            for m in all_markets:
                if m.get("status") != "active":
                    continue
                title_lower = m.get("title", "").lower()
                ticker_upper = m.get("ticker", "").upper()
                # Match hourly temp, daily high, and precipitation markets
                is_weather = (
                    "temperature" in title_lower
                    or "precip" in title_lower
                    or "temp" in title_lower
                    or "daily high" in title_lower
                    or "high temperature" in title_lower
                    or ticker_upper.startswith("KXHIGH")
                )
                if not is_weather:
                    continue

                ticker = m.get("ticker", "")
                if not ticker or ticker in seen_tickers:
                    continue

                hours_to_expiry = self._hours_to_expiry(m)
                if hours_to_expiry is None or hours_to_expiry <= 0:
                    continue
                # Daily high markets can have longer expiry windows (up to 12h);
                # hourly markets use the normal max_hours_to_expiry.
                is_daily_high = ticker_upper.startswith("KXHIGH") or "daily high" in title_lower
                max_hours = 12.0 if is_daily_high else self._max_hours_to_expiry
                if hours_to_expiry > max_hours:
                    continue

                volume = float(m.get("volume_fp", 0) or 0)
                if volume < 20:
                    continue

                seen_tickers.add(ticker)
                active_markets.append(m)

            active_markets.sort(
                key=lambda x: (
                    self._hours_to_expiry(x) or 9999.0,
                    -float(x.get("volume_fp", 0) or 0),
                )
            )

            self._active_markets = active_markets[: self._max_markets_per_scan]
            
            temp_count = sum(1 for m in self._active_markets if "temp" in m.get("title", "").lower())
            precip_count = len(self._active_markets) - temp_count

            now = time.time()
            should_log = (
                len(self._active_markets) != self._last_discovery_count
                or (now - self._last_discovery_log_ts) >= self._discovery_log_cooldown_seconds
            )
            if should_log:
                logger.info(
                    "Weather arb: tracking %d markets (temp=%d, precip=%d, cap=%d)",
                    len(self._active_markets),
                    temp_count,
                    precip_count,
                    self._max_markets_per_scan,
                )
                self._last_discovery_count = len(self._active_markets)
                self._last_discovery_log_ts = now

        except Exception as e:
            logger.error("Weather market discovery failed: %s", e)

    async def _evaluate_market(self, market: dict) -> Optional[Signal]:
        """
        Evaluate a single weather market against real-time NOAA data.
        """
        ticker = market.get("ticker", "")
        title = market.get("title", "")
        # Elections API uses dollar fields
        yes_bid_d = market.get("yes_bid_dollars")
        yes_ask_d = market.get("yes_ask_dollars")
        yes_bid = float(yes_bid_d) if yes_bid_d and float(yes_bid_d) > 0 else 0.0
        yes_ask = float(yes_ask_d) if yes_ask_d and float(yes_ask_d) > 0 else 0.0

        if yes_ask <= 0:
            return None

        last_signal_ts = self._last_signal_by_market.get(ticker, 0.0)
        if (time.time() - last_signal_ts) < self._signal_cooldown_seconds:
            return None

        if yes_bid > 0 and (yes_ask - yes_bid) > self._max_spread:
            return None

        hours_to_expiry = self._hours_to_expiry(market)
        if hours_to_expiry is None or hours_to_expiry <= 0:
            return None
        if hours_to_expiry > self._max_hours_to_expiry:
            return None

        # Parse the market to determine the station, strike, type, and sub_type.
        # Prefer floor_strike from market data (authoritative) over title regex.
        station_id, strike_value, market_type, sub_type = self._parse_weather_market(title, ticker, market)
        if not station_id:
            return None
        if market_type == "temperature" and strike_value <= 0:
            logger.debug("Weather arb: unable to parse strike for %s (%s)", ticker, title)
            return None

        # Fetch the latest observation from NOAA
        obs = await self._get_station_observation(station_id)
        if not obs:
            return None

        obs_age_seconds = (datetime.now(timezone.utc) - obs.timestamp).total_seconds()
        if obs_age_seconds > self._observation_max_age_seconds:
            logger.debug(
                "Weather arb: stale NOAA obs for %s (age=%.0fs)",
                station_id,
                obs_age_seconds,
            )
            return None

        # Determine observed value
        if market_type == "temperature":
            observed = obs.temperature_f
        elif market_type == "precipitation":
            observed = obs.precipitation_last_hour_mm
        else:
            return None

        if observed is None:
            return None

        # Calculate edge
        signal = self._calculate_signal(
            ticker=ticker,
            market_type=market_type,
            sub_type=sub_type,
            observed=observed,
            strike=strike_value,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            station_id=station_id,
            hours_to_expiry=hours_to_expiry,
        )

        if signal:
            self._last_signal_by_market[ticker] = time.time()
        return signal

    async def _get_station_observation(self, station_id: str) -> Optional[WeatherObservation]:
        now = time.time()
        cached = self._station_obs_cache.get(station_id)
        fetched_at = self._station_obs_fetched_at.get(station_id, 0.0)

        if cached and (now - fetched_at) <= self._obs_cache_ttl_seconds:
            return cached

        fresh = await self.noaa.get_latest_observation(station_id)
        if fresh is not None:
            self._station_obs_cache[station_id] = fresh
            self._station_obs_fetched_at[station_id] = now
            return fresh

        # Use recent cached obs if upstream is flaky.
        if cached and (now - fetched_at) <= (self._obs_cache_ttl_seconds * 3):
            return cached

        return None

    def _calculate_signal(
        self,
        ticker: str,
        market_type: str,
        observed: float,
        strike: float,
        yes_bid: float,
        yes_ask: float,
        station_id: str,
        hours_to_expiry: float = 1.0,
        sub_type: str = "hourly",
    ) -> Optional[Signal]:
        """
        Core signal generation logic with time-decay awareness.

        HOURLY markets:
          The required confidence margin scales dynamically with time-to-expiry.
          This prevents the bot from entering trades hours early based on
          momentary temperature readings that can drift before settlement.

        DAILY HIGH markets:
          Temperature naturally rises during the day and peaks around 2-4 PM.
          Betting NO on "daily high above X" in the morning is suicide because
          the high hasn't been recorded yet. We ONLY allow:
            - YES: when current temp already exceeds the strike (it's already the high)
            - NO:  only after 3 PM ET (≤2h to expiry) when the peak has passed
        """
        if market_type == "temperature" and sub_type == "daily_high":
            return self._calculate_daily_high_signal(
                ticker=ticker,
                observed=observed,
                strike=strike,
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                station_id=station_id,
                hours_to_expiry=hours_to_expiry,
            )

        # ── Standard hourly temperature logic ──────────────────────────

        # Dynamic margin: scales linearly from 3.0°F (at 0h) to 7.0°F (at 2h)
        # ULTRA CONSERVATIVE: requires overwhelming temp evidence to trade
        dynamic_margin = max(3.0, 3.0 + (hours_to_expiry * 4.0))

        # Time-based fair value multiplier (similar to latency arb theta)
        if hours_to_expiry <= 0.25:
            time_multiplier = 1.0    # 15 min or less — nearly settled
        elif hours_to_expiry <= 0.5:
            time_multiplier = 0.90   # 30 min — high confidence
        elif hours_to_expiry <= 1.0:
            time_multiplier = 0.75   # 1 hour — moderate
        else:
            time_multiplier = 0.55   # 1-2 hours — weather can shift

        if market_type == "temperature":
            # "Above X°F" market
            diff = observed - strike

            if diff > dynamic_margin:
                # Temperature is clearly above strike → YES should be high
                base_fair = min(0.95, 0.80 + (diff / 10.0) * 0.15)
                fair_value = 0.50 + (base_fair - 0.50) * time_multiplier
                if yes_ask < fair_value - 0.05:
                    edge = (fair_value - yes_ask) * 100  # cents
                    if edge >= self.min_edge_cents:
                        return Signal(
                            strategy="weather_arb",
                            action=Side.BUY,
                            contract_side=ContractSide.YES,
                            platform=Platform.KALSHI,
                            market_id=ticker,
                            target_price=yes_ask,
                            quantity=self._calculate_size(edge),
                            confidence=min(0.95, 0.70 + diff * 0.05) * time_multiplier,
                            reason=(
                                f"NOAA {station_id}: {observed:.1f}°F > strike {strike:.1f}°F "
                                f"by {diff:.1f}° (margin={dynamic_margin:.1f}°, "
                                f"fair={fair_value:.2f}, {hours_to_expiry:.1f}h left)"
                            ),
                        )

            elif diff < -dynamic_margin:
                # Temperature is clearly below strike → NO should be high
                base_fair_no = min(0.95, 0.80 + (abs(diff) / 10.0) * 0.15)
                fair_no = 0.50 + (base_fair_no - 0.50) * time_multiplier
                no_ask = 1.0 - yes_bid  # price to buy NO
                if no_ask < fair_no - 0.05:
                    edge = (fair_no - no_ask) * 100
                    if edge >= self.min_edge_cents:
                        return Signal(
                            strategy="weather_arb",
                            action=Side.BUY,
                            contract_side=ContractSide.NO,
                            platform=Platform.KALSHI,
                            market_id=ticker,
                            target_price=no_ask,
                            quantity=self._calculate_size(edge),
                            confidence=min(0.95, 0.70 + abs(diff) * 0.05) * time_multiplier,
                            reason=(
                                f"NOAA {station_id}: {observed:.1f}°F < strike {strike:.1f}°F "
                                f"by {abs(diff):.1f}° (margin={dynamic_margin:.1f}°, "
                                f"fair={fair_no:.2f}, {hours_to_expiry:.1f}h left)"
                            ),
                        )

        elif market_type == "precipitation":
            if observed is not None and observed > 0.01:
                # Rain detected → YES
                if yes_ask < 0.90:
                    edge = (0.95 - yes_ask) * 100
                    if edge >= self.min_edge_cents:
                        return Signal(
                            strategy="weather_arb",
                            action=Side.BUY,
                            contract_side=ContractSide.YES,
                            platform=Platform.KALSHI,
                            market_id=ticker,
                            target_price=yes_ask,
                            quantity=self._calculate_size(edge),
                            confidence=0.90,
                            reason=f"NOAA {station_id}: precip={observed:.2f}mm detected vs strike 0.01",
                        )

        return None

    def _calculate_daily_high_signal(
        self,
        ticker: str,
        observed: float,
        strike: float,
        yes_bid: float,
        yes_ask: float,
        station_id: str,
        hours_to_expiry: float,
    ) -> Optional[Signal]:
        """
        Signal logic for DAILY HIGH temperature markets.

        Key insight: daily high temperature is a running maximum.
        If it's 10 AM and current temp is 55°F vs a strike of 65°F,
        that tells us NOTHING — the high could easily hit 70°F by 3 PM.
        The old code would buy NO here and lose money every time.

        Rules:
          1. YES is allowed if observed temp ALREADY exceeds the strike.
             (The high is at least the current temp, so YES is guaranteed.)
          2. NO is ONLY allowed in the final 2 hours (after ~3 PM ET)
             when the daily peak has most likely already occurred.
             We also require a large margin (current temp well below strike).
          3. Otherwise: DO NOTHING. This is the critical fix.
        """
        diff = observed - strike

        # ── Case 1: current temp already exceeds the strike ────────────
        # The daily high is at least the current reading, so YES is locked in.
        if diff > 2.0:
            # Conservative fair value: very high because the outcome is decided
            fair_value = min(0.97, 0.88 + (diff / 20.0) * 0.09)
            if yes_ask < fair_value - 0.03:
                edge = (fair_value - yes_ask) * 100
                if edge >= self.min_edge_cents:
                    return Signal(
                        strategy="weather_arb",
                        action=Side.BUY,
                        contract_side=ContractSide.YES,
                        platform=Platform.KALSHI,
                        market_id=ticker,
                        target_price=yes_ask,
                        quantity=self._calculate_size(edge),
                        confidence=min(0.97, 0.90 + diff * 0.01),
                        reason=(
                            f"DAILY HIGH {station_id}: obs {observed:.1f}°F ALREADY > "
                            f"strike {strike:.1f}°F by {diff:.1f}° → YES locked"
                        ),
                    )

        # ── Case 2: late afternoon NO (peak has likely passed) ─────────
        # Only allow NO trades in the final 2 hours when the daily high
        # is effectively finalized. Require a massive margin.
        if hours_to_expiry <= 2.0 and diff < -5.0:
            # Late in the day and temp is well below strike
            # More conservative: need bigger margin with more time
            required_margin = max(5.0, 5.0 + (hours_to_expiry * 3.0))
            if diff < -required_margin:
                base_fair_no = min(0.90, 0.70 + (abs(diff) / 15.0) * 0.15)
                # Further discount with time — even 2h out is uncertain
                time_mult = max(0.60, 1.0 - hours_to_expiry * 0.20)
                fair_no = 0.50 + (base_fair_no - 0.50) * time_mult
                no_ask = 1.0 - yes_bid
                if no_ask > 0 and no_ask < fair_no - 0.05:
                    edge = (fair_no - no_ask) * 100
                    if edge >= self.min_edge_cents:
                        return Signal(
                            strategy="weather_arb",
                            action=Side.BUY,
                            contract_side=ContractSide.NO,
                            platform=Platform.KALSHI,
                            market_id=ticker,
                            target_price=no_ask,
                            quantity=self._calculate_size(edge),
                            confidence=min(0.90, 0.65 + abs(diff) * 0.03) * time_mult,
                            reason=(
                                f"DAILY HIGH {station_id}: obs {observed:.1f}°F < "
                                f"strike {strike:.1f}°F by {abs(diff):.1f}° "
                                f"(late PM, {hours_to_expiry:.1f}h left, margin={required_margin:.1f}°)"
                            ),
                        )

        # ── Case 3: too early / inconclusive → DO NOTHING ──────────────
        # This is the critical path that prevents the old systematic losses.
        logger.debug(
            "Weather arb: DAILY HIGH %s skipped — obs=%.1f°F, strike=%.1f°F, "
            "%.1fh left (too early/uncertain for NO, diff too small for YES)",
            ticker, observed, strike, hours_to_expiry,
        )
        return None

    def _calculate_size(self, edge: float) -> int:
        return max(1, min(100, int(edge * 2)))

    def _parse_weather_market(
        self, title: str, ticker: str, market: dict | None = None
    ) -> tuple[Optional[str], float, str, str]:
        """
        Parse a Kalshi weather market to extract station, strike, type,
        and sub_type.

        Returns:
            (station_id, strike, market_type, sub_type)
            sub_type is "hourly" or "daily_high"

        Uses the market's floor_strike field (authoritative) when available,
        falling back to regex on the title / ticker.

        Example titles:
          "Will the temp in NYC be above 75.99° on Apr 14, 2026 at 1am EDT?"
          "Will the daily high in Denver be above 65°F on Apr 14, 2026?"
        Example tickers:
          "KXTEMPNYCH-26APR1401-T75.99"   (hourly)
          "KXHIGHDEN-26APR14-T65"         (daily high)
        """
        import re
        title_lower = title.lower()

        # ── Determine station ──────────────────────────────────────────
        station_id = None
        for location, sid in STATION_MAP.items():
            if location in title_lower:
                station_id = sid
                break
        # Try METAR codes embedded in title (e.g. "jfk", "ord")
        if not station_id:
            for sid in STATION_MAP.values():
                if sid.lower()[1:] in title_lower:  # "nyc" from "KNYC"
                    station_id = sid
                    break
        # Try to derive from series ticker (e.g. KXTEMPNYCH → NYC, KXHIGHDEN → DEN)
        if not station_id:
            ticker_upper = ticker.upper()
            if "NYC" in ticker_upper or "NYD" in ticker_upper:
                station_id = "KNYC"
            elif "CHI" in ticker_upper or "ORD" in ticker_upper:
                station_id = "KORD"
            elif "LAX" in ticker_upper:
                station_id = "KLAX"
            elif "MIA" in ticker_upper:
                station_id = "KMIA"
            elif "DFW" in ticker_upper or "DAL" in ticker_upper:
                station_id = "KDFW"
            elif "DEN" in ticker_upper:
                station_id = "KDEN"
            elif "SEA" in ticker_upper:
                station_id = "KSEA"
            elif "ATL" in ticker_upper:
                station_id = "KATL"

        # ── Determine market type and sub_type ─────────────────────────
        market_type = ""
        sub_type = "hourly"  # Default
        strike = 0.0

        if "temperature" in title_lower or "temp" in title_lower or "high" in title_lower:
            market_type = "temperature"
        elif "precipitation" in title_lower or "rain" in title_lower or "precip" in title_lower:
            market_type = "precipitation"
            strike = 0.01  # Default: any measurable precipitation

        # Detect daily high markets:
        #   - Ticker starts with KXHIGH (KXHIGHDEN, KXHIGHMIA, KXHIGHLAX, etc.)
        #   - Title contains "daily high" or "high temperature"
        ticker_upper = ticker.upper()
        if ticker_upper.startswith("KXHIGH"):
            sub_type = "daily_high"
            market_type = "temperature"  # Always temperature for high markets
        elif "daily high" in title_lower or "high temperature" in title_lower:
            sub_type = "daily_high"
        # KXTEMPNYCH is hourly (default), no change needed

        # ── Determine strike ───────────────────────────────────────────
        if market_type == "temperature":
            # 1. Prefer floor_strike from market data (authoritative Kalshi field)
            if market and market.get("floor_strike") is not None:
                try:
                    strike = float(market["floor_strike"])
                except (TypeError, ValueError):
                    strike = 0.0

            # 2. Fallback: parse from ticker suffix (e.g. "-T75.99")
            if strike <= 0:
                match = re.search(r'-T([\d.]+)$', ticker)
                if match:
                    strike = float(match.group(1))

            # 3. Fallback: parse from title (e.g. "above 75.99°")
            if strike <= 0:
                match = re.search(r'above\s+([\d.]+)', title_lower)
                if match:
                    strike = float(match.group(1))
                else:
                    match = re.search(r'([\d.]+)\s*°', title)
                    if match:
                        strike = float(match.group(1))

        return station_id, strike, market_type, sub_type

    def get_stats(self) -> dict:
        return {
            "active_markets": len(self._active_markets),
            "signals_generated": len(self._signals_generated),
            "stations_monitored": list(STATION_MAP.values()),
        }

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
