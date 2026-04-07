"""
Macro News Sniper Strategy

Exploits the critical seconds around major economic data releases
(CPI, Non-Farm Payrolls, Fed Decisions) by monitoring official
government data sources and trading Kalshi markets before the
broader market can react.

How it works:
  1. Before a release (e.g. CPI at 8:30 AM ET), the bot enters
     "sniper mode" — polling the BLS website every 500ms.
  2. The instant the number appears, we compare it to the consensus
     estimate and the Kalshi market's implied probability.
  3. If CPI comes in at 3.5% vs consensus of 3.2%, and Kalshi
     "CPI above 3.3%" is still at 60¢, we sweep YES.

Target Kalshi series:
  - KXCPIYOY      : CPI Year-over-Year
  - KXJOBS        : Non-Farm Payrolls
  - KXCLAIMS      : Weekly Jobless Claims
  - KXFEDDECISION : Federal Reserve Interest Rate Decision

External data sources:
  - BLS:  https://www.bls.gov/news.release/cpi.nr0.htm
  - FRED: https://fred.stlouisfed.org (Federal Reserve Economic Data)
  - BEA:  https://www.bea.gov/ (GDP, PCE)
"""

import asyncio
import logging
import time
import re
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)


# ── Release Schedule ────────────────────────────────────────────────────────

@dataclass
class EconomicRelease:
    """Defines a scheduled economic data release."""
    name: str
    kalshi_series: str
    release_time_utc: datetime      # Exact release time
    source_url: str                 # Official URL to poll
    market_ticker: Optional[str] = None
    consensus_estimate: Optional[float] = None
    actual_value: Optional[float] = None
    is_released: bool = False


# CPI release page patterns for scraping
BLS_CPI_URL = "https://www.bls.gov/news.release/cpi.nr0.htm"
BLS_JOBS_URL = "https://www.bls.gov/news.release/empsit.nr0.htm"
BLS_CLAIMS_URL = "https://www.dol.gov/ui/data.pdf"
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"

# Pattern to extract CPI numbers from BLS press release
CPI_PATTERN = re.compile(
    r'(?:increased|rose|advanced|declined|decreased|unchanged)\s+'
    r'([\d.]+)\s*percent',
    re.IGNORECASE,
)

# Pattern for non-farm payrolls
JOBS_PATTERN = re.compile(
    r'(?:nonfarm payroll|total nonfarm).*?(?:increased|rose|added|changed).*?'
    r'([\d,]+)',
    re.IGNORECASE,
)

# Pattern for jobless claims
CLAIMS_PATTERN = re.compile(
    r'(?:initial claims|seasonally adjusted).*?([\d,]+)',
    re.IGNORECASE,
)


from models import Signal, Side, ContractSide, Platform

class MacroNewsScraper:
    """
    Scrapes official government websites for economic data releases.
    """

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=5.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PredictionBot/1.0",
            },
        )

    async def scrape_cpi(self) -> Optional[float]:
        """
        Scrape the BLS CPI press release page for the latest CPI figure.
        Returns the YoY CPI percentage change, or None if not found.
        """
        try:
            resp = await self._client.get(BLS_CPI_URL)
            resp.raise_for_status()
            text = resp.text

            # Look for the all-items YoY figure
            # Typical BLS text: "The Consumer Price Index for All Urban Consumers
            # increased 3.5 percent over the last 12 months"
            yoy_pattern = re.compile(
                r'(?:over the last 12 months|12-month|year.over.year|past year)'
                r'.*?(?:before seasonal adjustment)?'
                r'.*?([\d.]+)\s*percent',
                re.IGNORECASE | re.DOTALL,
            )

            # Try the simpler pattern first
            matches = CPI_PATTERN.findall(text)
            if matches:
                return float(matches[0])

            # Try the YoY-specific pattern
            match = yoy_pattern.search(text)
            if match:
                return float(match.group(1))

            return None

        except Exception as e:
            logger.error("CPI scrape failed: %s", e)
            return None

    async def scrape_nonfarm_payrolls(self) -> Optional[int]:
        """
        Scrape the BLS Employment Situation press release for NFP number.
        Returns the change in non-farm payrolls (thousands), or None.
        """
        try:
            resp = await self._client.get(BLS_JOBS_URL)
            resp.raise_for_status()
            text = resp.text

            matches = JOBS_PATTERN.findall(text)
            if matches:
                # Remove commas and convert
                return int(matches[0].replace(",", ""))

            return None

        except Exception as e:
            logger.error("NFP scrape failed: %s", e)
            return None

    async def scrape_jobless_claims(self) -> Optional[int]:
        """
        Scrape the DOL website for initial jobless claims.
        Returns the seasonally adjusted initial claims number.
        """
        try:
            # The DOL publishes claims in a report; we try the main page
            resp = await self._client.get(
                "https://www.dol.gov/newsroom/releases"
            )
            resp.raise_for_status()
            text = resp.text

            matches = CLAIMS_PATTERN.findall(text)
            if matches:
                return int(matches[0].replace(",", ""))

            return None

        except Exception as e:
            logger.error("Claims scrape failed: %s", e)
            return None

    async def close(self):
        await self._client.aclose()


class MacroNewsStrategy:
    """
    Monitors upcoming economic releases and enters "sniper mode"
    around release times to capture the initial price dislocation.
    """

    def __init__(self, kalshi_exchange=None):
        self.kalshi = kalshi_exchange
        self.scraper = MacroNewsScraper()
        self._active = False
        self._series_market_cache: dict[str, str] = {}

        # Upcoming releases (populated by schedule manager)
        self.upcoming_releases: list[EconomicRelease] = []

        # Sniper mode config
        self.pre_release_window_seconds = 300   # Enter sniper mode 5 min before
        self.poll_interval_fast_ms = 500        # Poll every 500ms during sniper mode
        self.poll_interval_normal_s = 60        # Poll every 60s when idle
        self.min_surprise_pct = 0.1             # Minimum surprise to trigger signal

        # Stats
        self._signals_generated: list[Signal] = []
        self._releases_captured = 0

    async def start(self):
        self._active = True
        try:
            await self.discover_markets()
        except Exception as e:
            logger.warning("Macro discovery warmup failed: %s", e)
        logger.info("Macro news sniper: started")

    async def stop(self):
        self._active = False
        await self.scraper.close()

    def add_release(self, release: EconomicRelease):
        """Schedule an upcoming economic release to monitor."""
        self.upcoming_releases.append(release)
        logger.info(
            "Macro sniper: scheduled %s at %s (consensus=%.2f)",
            release.name, release.release_time_utc.isoformat(),
            release.consensus_estimate or 0,
        )

    async def run_loop(self, signal_callback=None):
        """
        Main loop: check if we're near a release window and act accordingly.
        """
        while self._active:
            now = datetime.now(timezone.utc)

            for release in self.upcoming_releases:
                if release.is_released:
                    continue

                time_to_release = (release.release_time_utc - now).total_seconds()

                if -60 < time_to_release < self.pre_release_window_seconds:
                    # We're in the sniper window!
                    logger.info(
                        "SNIPER MODE: %s in %.0f seconds",
                        release.name, time_to_release,
                    )
                    signal = await self._sniper_mode(release)
                    if signal and signal_callback:
                        await signal_callback(signal)

            await asyncio.sleep(self.poll_interval_normal_s)

    async def _sniper_mode(self, release: EconomicRelease) -> Optional[Signal]:
        """
        Enter rapid-polling mode for a specific release.
        Polls the source URL every 500ms until the data appears.
        """
        logger.info("Entering sniper mode for %s", release.name)
        max_attempts = 600  # 5 minutes of polling at 500ms = 600 attempts

        for attempt in range(max_attempts):
            actual = await self._fetch_actual_value(release)

            if actual is not None:
                release.actual_value = actual
                release.is_released = True
                self._releases_captured += 1

                logger.info(
                    "DATA CAPTURED: %s = %.2f (consensus=%.2f)",
                    release.name, actual,
                    release.consensus_estimate or 0,
                )

                # Generate signal
                return self._generate_signal(release)

            await asyncio.sleep(self.poll_interval_fast_ms / 1000.0)

        logger.warning("Sniper mode timed out for %s", release.name)
        return None

    async def _fetch_actual_value(self, release: EconomicRelease) -> Optional[float]:
        """Attempt to fetch the actual released value."""
        if "cpi" in release.name.lower():
            return await self.scraper.scrape_cpi()
        elif "payroll" in release.name.lower() or "nfp" in release.name.lower():
            result = await self.scraper.scrape_nonfarm_payrolls()
            return float(result) if result else None
        elif "claims" in release.name.lower():
            result = await self.scraper.scrape_jobless_claims()
            return float(result) if result else None
        return None

    def _generate_signal(self, release: EconomicRelease) -> Optional[Signal]:
        """
        Compare actual vs consensus and generate a trading signal.
        """
        if release.actual_value is None or release.consensus_estimate is None:
            return None

        actual = release.actual_value
        consensus = release.consensus_estimate
        surprise = actual - consensus
        surprise_pct = abs(surprise / consensus) if consensus != 0 else 0

        if surprise_pct < self.min_surprise_pct:
            logger.info(
                "Macro: %s surprise too small (%.2f%%), skipping",
                release.name, surprise_pct * 100,
            )
            return None

        # Determine direction
        if surprise > 0:
            direction = "ABOVE"
            action = Side.BUY
            contract_side = ContractSide.YES
        else:
            direction = "BELOW"
            action = Side.BUY
            contract_side = ContractSide.NO

        # Confidence scales with surprise magnitude
        confidence = min(0.95, 0.60 + surprise_pct * 2)

        market_ticker = (
            release.market_ticker
            or self._series_market_cache.get(release.kalshi_series)
            or release.kalshi_series
        )

        signal = Signal(
            strategy="macro_news",
            action=action,
            contract_side=contract_side,
            platform=Platform.KALSHI,
            market_id=market_ticker,
            target_price=0.95,  # Sweep the book
            quantity=max(1, int(surprise_pct * 100)),
            confidence=confidence,
            reason=(
                f"{release.name}: actual={actual:.2f} vs consensus={consensus:.2f} "
                f"({direction}, surprise={surprise_pct:.1%})"
            )
        )

        self._signals_generated.append(signal)
        logger.info(
            "MACRO SIGNAL: %s %s -- %s",
            signal.action.value, signal.market_id, signal.reason,
        )

        return signal

    async def discover_markets(self) -> list[dict]:
        """Find active macro markets on Kalshi."""
        if not self.kalshi:
            return []

        markets = []
        for series in ["KXCPIYOY", "KXJOBS", "KXCLAIMS", "KXFEDDECISION"]:
            try:
                found = await self.kalshi.fetch_markets(category=series)
                markets.extend(found)
            except Exception as e:
                logger.error("Failed to discover %s markets: %s", series, e)

        active = [m for m in markets if (m.get("status") or "").lower() in {"active", "open"}]

        # Cache one tradable ticker per series so generated signals target real markets.
        by_series: dict[str, str] = {}
        for m in active:
            t = m.get("ticker", "")
            if not t:
                continue
            for series in ["KXCPIYOY", "KXJOBS", "KXCLAIMS", "KXFEDDECISION"]:
                if t.startswith(series):
                    by_series.setdefault(series, t)
                    break
        self._series_market_cache.update(by_series)

        return active

    def get_stats(self) -> dict:
        return {
            "upcoming_releases": len([r for r in self.upcoming_releases if not r.is_released]),
            "releases_captured": self._releases_captured,
            "signals_generated": len(self._signals_generated),
        }
