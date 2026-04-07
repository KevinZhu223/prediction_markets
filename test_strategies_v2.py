"""
Test suite for Strategies V2 -- Expansion Modules.

Run with: python test_strategies_v2.py

Tests are designed to work WITHOUT affecting the running main bot.
Each strategy is tested in isolation using its own data sources.
"""

import asyncio
import sys
import time

sys.path.insert(0, ".")

from config import Config

# -- Helpers -----------------------------------------------------------------

PASS = "[PASS]"
FAIL = "[FAIL]"
SKIP = "[SKIP]"
results = []

def report(name, passed, detail=""):
    status = PASS if passed else FAIL
    results.append((name, passed))
    print(f"  {status} {name}" + (f" -- {detail}" if detail else ""))

def report_skip(name, reason):
    results.append((name, None))
    print(f"  {SKIP} {name} -- {reason}")


# ===========================================================================
# Test 1: Weather Arb -- NOAA API
# ===========================================================================

async def test_weather_arb():
    print("\n=== Test 1: Weather Arb (NOAA) ===")
    from strategies_v2.weather_arb import NOAAClient, WeatherArbStrategy

    noaa = NOAAClient()

    # Test 1a: Fetch latest observation from JFK
    obs = await noaa.get_latest_observation("KJFK")
    report(
        "NOAA fetch KJFK observation",
        obs is not None and obs.temperature_f is not None,
        f"temp={obs.temperature_f:.1f}F, station={obs.station_id}" if obs else "No data",
    )

    # Test 1b: Fetch from Chicago O'Hare
    obs_ord = await noaa.get_latest_observation("KORD")
    report(
        "NOAA fetch KORD observation",
        obs_ord is not None,
        f"temp={obs_ord.temperature_f:.1f}F" if obs_ord and obs_ord.temperature_f else "No temp data",
    )

    # Test 1c: Fetch recent observations (trend data)
    recent = await noaa.get_recent_observations("KJFK", limit=3)
    report(
        "NOAA recent observations",
        len(recent) > 0,
        f"got {len(recent)} observations",
    )

    # Test 1d: Signal generation logic (synthetic)
    strategy = WeatherArbStrategy()
    signal = strategy._calculate_signal(
        ticker="KXTEMPH-TEST",
        market_type="temperature",
        observed=82.0,       # Station reads 82F
        strike=75.0,          # Market asks "above 75F?"
        yes_bid=0.70,
        yes_ask=0.75,         # Market at 75c but should be ~95c
        station_id="KJFK",
    )
    report(
        "Weather signal: temp above strike -> BUY_YES",
        signal is not None and signal.contract_side.value == "yes",
        f"conf={signal.confidence:.2f}" if signal else "No signal",
    )

    # Test 1e: Signal for temp below strike
    signal_below = strategy._calculate_signal(
        ticker="KXTEMPH-TEST2",
        market_type="temperature",
        observed=68.0,        # Station reads 68F
        strike=75.0,          # Market asks "above 75F?"
        yes_bid=0.35,
        yes_ask=0.40,         # Market at 40c YES, NO costs 65c
        station_id="KJFK",
    )
    report(
        "Weather signal: temp below strike -> BUY_NO",
        signal_below is not None and signal_below.contract_side.value == "no",
        "Signal success" if signal_below else "No signal",
    )

    # Test 1f: Market title parser
    station, strike, mtype = strategy._parse_weather_market(
        "Temperature at JFK above 75F at 2 PM ET", "KXTEMPH-TEST"
    )
    report(
        "Weather market parser",
        station == "KJFK" and strike == 75.0 and mtype == "temperature",
        f"station={station}, strike={strike}, type={mtype}",
    )

    await noaa.close()


# ===========================================================================
# Test 2: Macro News Sniper
# ===========================================================================

async def test_macro_news():
    print("\n=== Test 2: Macro News Sniper ===")
    from strategies_v2.macro_news import MacroNewsStrategy, MacroNewsScraper, EconomicRelease
    from datetime import datetime, timezone

    scraper = MacroNewsScraper()

    # Test 2a: BLS CPI page is reachable
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get("https://www.bls.gov/cpi/")
            if resp.status_code == 200:
                report("BLS CPI page reachable", True, f"status={resp.status_code}")
            elif resp.status_code in (403, 429, 503):
                report_skip("BLS CPI page reachable", f"blocked/limited status={resp.status_code}")
            else:
                report("BLS CPI page reachable", False, f"status={resp.status_code}")
    except Exception as e:
        report_skip("BLS CPI page reachable", f"network error: {e}")

    # Test 2b: Signal generation logic (synthetic)
    strategy = MacroNewsStrategy()
    release = EconomicRelease(
        name="CPI YoY",
        kalshi_series="KXCPIYOY",
        release_time_utc=datetime.now(timezone.utc),
        source_url="https://www.bls.gov/cpi/",
        consensus_estimate=3.0,
        actual_value=3.5,   # ~16.7% surprise, well above 10% threshold
        is_released=True,
    )

    signal = strategy._generate_signal(release)
    report(
        "Macro signal: CPI above consensus -> BUY_YES",
        signal is not None and signal.contract_side.value == "yes",
        f"actual={release.actual_value} vs consensus={release.consensus_estimate}" if signal else "No signal",
    )

    # Test 2c: Below consensus
    release_below = EconomicRelease(
        name="CPI YoY",
        kalshi_series="KXCPIYOY",
        release_time_utc=datetime.now(timezone.utc),
        source_url="",
        consensus_estimate=3.5,
        actual_value=3.0,
        is_released=True,
    )
    signal_below = strategy._generate_signal(release_below)
    report(
        "Macro signal: CPI below consensus -> BUY_NO",
        signal_below is not None and signal_below.contract_side.value == "no",
        "Signal success" if signal_below else "No signal",
    )

    # Test 2d: No signal when surprise is tiny
    release_flat = EconomicRelease(
        name="CPI YoY",
        kalshi_series="KXCPIYOY",
        release_time_utc=datetime.now(timezone.utc),
        source_url="",
        consensus_estimate=3.2,
        actual_value=3.21,  # Only 0.3% surprise
        is_released=True,
    )
    signal_flat = strategy._generate_signal(release_flat)
    report(
        "Macro: no signal on tiny surprise",
        signal_flat is None,
        "correctly suppressed" if signal_flat is None else f"unexpected signal: {signal_flat.action}",
    )

    await scraper.close()


# ===========================================================================
# Test 3: Equity Index Close Arb
# ===========================================================================

async def test_equity_index():
    print("\n=== Test 3: Equity Index Arb ===")
    from strategies_v2.equity_index_arb import EquityIndexArbStrategy, YahooFinanceFeed

    feed = YahooFinanceFeed()

    # Test 3a: Yahoo Finance SPX price fetch
    obs = await feed.get_price("^GSPC")
    report(
        "Yahoo Finance S&P 500",
        obs is not None and obs.price > 1000,
        f"SPX={obs.price:.1f}, change={obs.change_pct:.2f}%" if obs else "No data",
    )

    # Test 3b: Yahoo Finance NDX price fetch
    obs_ndx = await feed.get_price("^NDX")
    report(
        "Yahoo Finance Nasdaq-100",
        obs_ndx is not None and obs_ndx.price > 5000,
        f"NDX={obs_ndx.price:.1f}" if obs_ndx else "No data",
    )

    # Test 3c: Signal logic -- index clearly above strike
    strategy = EquityIndexArbStrategy()
    signal = strategy._evaluate_index_market(
        index_name="SPX",
        index_price=5250.0,
        market={
            "ticker": "KXINXU-TEST",
            "title": "S&P 500 above 5,200 on Friday?",
            "yes_bid": 60,
            "yes_ask": 65,
        },
        minutes_to_close=5.0,
    )
    report(
        "Index signal: SPX above strike -> BUY_YES",
        signal is not None and signal.contract_side.value == "yes",
        "Signal success" if signal else "No signal",
    )

    # Test 3d: Signal -- index clearly below strike
    signal_below = strategy._evaluate_index_market(
        index_name="SPX",
        index_price=5100.0,
        market={
            "ticker": "KXINXU-TEST2",
            "title": "S&P 500 above 5,200 on Friday?",
            "yes_bid": 40,
            "yes_ask": 45,
        },
        minutes_to_close=3.0,
    )
    report(
        "Index signal: SPX below strike -> BUY_NO",
        signal_below is not None and signal_below.contract_side.value == "no",
        "Signal success" if signal_below else "No signal",
    )

    # Test 3e: Strike parser
    strike = strategy._parse_strike_from_title("S&P 500 above 5,200 on Friday?")
    report(
        "Strike parser",
        strike == 5200.0,
        f"parsed={strike}",
    )

    await feed.close()


# ===========================================================================
# Test 4: Transcript Sniper
# ===========================================================================

async def test_transcript_sniper():
    print("\n=== Test 4: Transcript Sniper ===")
    from strategies_v2.transcript_sniper import (
        TranscriptSniperStrategy, MentionTarget, KeywordMatcher,
    )

    # Test 4a: Keyword matcher
    matcher = KeywordMatcher()
    matcher.add_keywords("KXSNLMENTION", ["Elon Musk", "Taylor Swift", "Bitcoin"])

    result = matcher.check("KXSNLMENTION", "And now, here's a sketch about Elon Musk going to Mars!")
    report(
        "Keyword matcher: finds 'Elon Musk'",
        result is not None and result[0] == "Elon Musk",
        f"matched='{result[0]}'" if result else "No match",
    )

    # Test 4b: No false positive
    result_neg = matcher.check("KXSNLMENTION", "The weather today is sunny and warm.")
    report(
        "Keyword matcher: no false positive",
        result_neg is None,
    )

    # Test 4c: Simulate transcript processing
    strategy = TranscriptSniperStrategy(openai_api_key=Config.OPENAI_API_KEY)
    strategy.add_target(MentionTarget(
        kalshi_series="KXSNLMENTION-TEST",
        keywords=["Elon Musk", "SpaceX"],
        stream_url="https://youtube.com/test",
        show_name="SNL Test",
    ))

    signals = await strategy.simulate_transcript(
        "Good evening everyone, tonight on the show we have a very special guest, Elon Musk!"
    )
    report(
        "Transcript simulation: detect mention -> signal",
        len(signals) > 0 and "Elon Musk" in signals[0].reason,
        f"reason='{signals[0].reason}', action={signals[0].action.value}" if signals else "No signal",
    )

    # Test 4d: Keyword extraction from market title
    keywords = strategy._extract_keywords_from_title("Will SNL mention Taylor Swift?")
    report(
        "Title keyword extraction",
        "Taylor Swift" in keywords,
        f"extracted={keywords}",
    )

    # Test 4e: No duplicate detection
    signals2 = await strategy.simulate_transcript("Elon Musk appears again!")
    report(
        "No duplicate detection",
        len(signals2) == 0,
        "correctly suppressed duplicate" if not signals2 else f"unexpected: {len(signals2)} signals",
    )


# ===========================================================================
# Run all V2 tests
# ===========================================================================

async def run_all():
    print("=" * 60)
    print("  Strategies V2 -- Expansion Module Tests")
    print("=" * 60)

    await test_weather_arb()
    await test_macro_news()
    await test_equity_index()
    await test_transcript_sniper()

    # Summary
    print("\n" + "=" * 60)
    passed = sum(1 for _, p in results if p is True)
    failed = sum(1 for _, p in results if p is False)
    skipped = sum(1 for _, p in results if p is None)
    print(f"  Results: {passed} passed, {failed} failed, {skipped} skipped / {len(results)} total")
    print("=" * 60)

    if failed > 0:
        print("\n  FAILED tests:")
        for name, p in results:
            if p is False:
                print(f"    - {name}")

    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_all())
    sys.exit(0 if success else 1)
