"""
Test suite for the Prediction Market Bot.
Run with: python test_bot.py
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")

from config import Config
from models import Signal, Side, Platform, ContractSide, OrderBook, OrderBookLevel, SpotPriceUpdate
from engine.risk_manager import RiskManager
from exchanges.kalshi_wrapper import KalshiExchange
from data.crypto_feed import CryptoFeed

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


# -- Test 1: Config ----------------------------------------------------------

def _run_config_checks():
    print("\n=== Test 1: Configuration ===")
    warnings = Config.validate()
    report("Config loads without crash", True)
    report("PAPER_TRADING is true", Config.PAPER_TRADING, f"value={Config.PAPER_TRADING}")

    has_kalshi = bool(Config.KALSHI_API_KEY)
    has_openai = bool(Config.OPENAI_API_KEY)
    report("KALSHI_API_KEY present", has_kalshi)
    report("OPENAI_API_KEY present", has_openai)
    report("Risk params loaded",
           Config.KELLY_FRACTION > 0 and Config.MAX_DRAWDOWN_PCT > 0,
           f"kelly={Config.KELLY_FRACTION}, max_dd={Config.MAX_DRAWDOWN_PCT}")

    return has_kalshi, has_openai


def test_config():
    _run_config_checks()


# -- Test 2: Risk Manager ---------------------------------------------------

def test_risk_manager():
    print("\n=== Test 2: Risk Manager ===")
    state_path = Path("logs/test_portfolio_state.json")
    if state_path.exists():
        state_path.unlink()

    rm = RiskManager(
        initial_equity=1000.0,
        state_file=str(state_path),
        session_name="test_suite",
    )

    # Test Kelly sizing
    signal = Signal(
        strategy="test",
        action=Side.BUY,
        contract_side=ContractSide.YES,
        platform=Platform.KALSHI,
        market_id="TEST-MKT",
        target_price=0.40,
        quantity=50,
        confidence=0.65,
        reason="test signal",
    )

    approved, reason, qty = rm.validate_signal(signal)
    report("Signal validation works", approved, f"qty={qty}, reason={reason}")

    # Test low confidence rejection
    low_conf_signal = Signal(
        strategy="test", action=Side.BUY, contract_side=ContractSide.YES,
        platform=Platform.KALSHI, market_id="TEST-LOW", target_price=0.50, quantity=10,
        confidence=max(0.0, rm.min_signal_confidence - 0.05), reason="low confidence",
    )
    approved_low, reason_low, _ = rm.validate_signal(low_conf_signal)
    report("Low confidence rejected", not approved_low, f"reason={reason_low}")

    # Test drawdown circuit breaker
    trigger_drawdown = min(rm.max_drawdown_pct + 0.05, 0.95)
    rm.current_equity = rm.peak_equity * (1 - trigger_drawdown)
    approved_dd, reason_dd, _ = rm.validate_signal(signal)
    report("Drawdown circuit breaker triggers", not approved_dd, f"reason={reason_dd}")

    # Reset and verify
    rm.reset_circuit_breaker()
    rm.current_equity = 1000.0
    rm.peak_equity = 1000.0
    approved_reset, _, _ = rm.validate_signal(signal)
    report("Circuit breaker reset works", approved_reset)

    # Test toxic flow detection
    is_toxic = rm.detect_toxic_flow("TEST-MKT", 10)
    report("Normal volume not toxic", not is_toxic)

    for _ in range(5):
        rm.detect_toxic_flow("TEST-MKT", 10)
    is_toxic = rm.detect_toxic_flow("TEST-MKT", 200)
    report("Volume spike detected as toxic", is_toxic)

    # Stats
    stats = rm.get_stats()
    report("Stats generation works", "equity" in stats and "pnl" in stats,
           f"equity={stats['equity']}")
    report(
        "Telemetry stats available",
        "open_notional_cost_basis" in stats and "risk_profile" in stats,
        f"profile={stats.get('risk_profile')}",
    )

    if state_path.exists():
        state_path.unlink()


# -- Test 3: Kalshi Connection -----------------------------------------------

async def test_kalshi(has_key: bool):
    print("\n=== Test 3: Kalshi API Connection ===")
    if not has_key:
        report_skip("Kalshi connect", "KALSHI_API_KEY not set")
        report_skip("Kalshi fetch balance", "KALSHI_API_KEY not set")
        report_skip("Kalshi fetch markets", "KALSHI_API_KEY not set")
        return

    kalshi = KalshiExchange()
    try:
        await kalshi.connect()
        report("Kalshi connect", True)
    except Exception as e:
        report("Kalshi connect", False, str(e))
        return

    try:
        balance = await kalshi.fetch_balance()
        report("Kalshi fetch balance", balance >= 0, f"balance=${balance:.2f}")
    except Exception as e:
        report("Kalshi fetch balance", False, str(e))

    markets = []
    try:
        markets = await kalshi.fetch_markets()
        report("Kalshi fetch markets", len(markets) > 0, f"found {len(markets)} markets")
    except Exception as e:
        report("Kalshi fetch markets", False, str(e))

    # Test order book fetch for first available market
    try:
        if markets:
            ticker = markets[0].get("ticker", "")
            ob = await kalshi.fetch_order_book(ticker)
            report("Kalshi fetch orderbook", ob is not None,
                   f"market={ticker}, bids={len(ob.bids)}, asks={len(ob.asks)}")
    except Exception as e:
        report("Kalshi fetch orderbook", False, str(e))

    # Test ticker discovery
    try:
        ticker = await kalshi.get_active_series_ticker("KXBTC15M")
        if ticker:
            report("Ticker discovery works", True, f"series=KXBTC15M ticker={ticker}")
        else:
            report_skip("Ticker discovery", "No active KXBTC15M found")
    except Exception as e:
        report("Ticker discovery failed", False, str(e))

    await kalshi.disconnect()


# -- Test 4: Coinbase Crypto Feed -------------------------------------------

async def test_crypto_feed():
    print("\n=== Test 4: Coinbase Crypto Feed ===")
    from data.crypto_feed import CryptoFeed
    feed = CryptoFeed(symbols=["btcusdt"], jump_threshold=50.0)

    prices_received = []

    async def on_update(update, delta):
        prices_received.append(update.price)

    feed.on_update(on_update)

    task = asyncio.create_task(feed.start())
    try:
        await asyncio.sleep(5)
    except Exception:
        pass

    await feed.stop()
    try:
        await asyncio.wait_for(task, timeout=5)
    except asyncio.TimeoutError:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    
    has_data = len(prices_received) > 0
    report("Coinbase feed receives data", has_data, f"received {len(prices_received)} price updates")


# -- Test 6: Latency Arb Signal Logic ----------------------------------------

def test_latency_arb_logic():
    print("\n=== Test 6: Latency Arb Signal Logic ===")
    from strategies.latency_arb import LatencyArbStrategy

    class MockAggregator:
        def __init__(self):
            self.binance = type('obj', (object,), {'on_update': lambda self, cb: None})()
            self._order_books = {}
            self.kalshi = None

        def get_order_book(self, market_id):
            return self._order_books.get(market_id)

    agg = MockAggregator()
    strategy = LatencyArbStrategy(agg)
    ticker = "KXBTC-TEST-15M"
    
    # Simulate orderbook
    agg._order_books[ticker] = OrderBook(
        market_id=ticker,
        platform=Platform.KALSHI,
        bids=[OrderBookLevel(price=0.45, quantity=100)],
        asks=[OrderBookLevel(price=0.48, quantity=100)],
    )

    # Test UP jump -> Buy YES
    up_signals = strategy._generate_up_signals("BTCUSDT", 100.0, ticker)
    report("Up jump generates YES signal", len(up_signals) > 0 and up_signals[0].contract_side == ContractSide.YES)

    # Test DOWN jump -> Buy NO
    down_signals = strategy._generate_down_signals("BTCUSDT", -100.0, ticker)
    report("Down jump generates NO signal", len(down_signals) > 0 and down_signals[0].contract_side == ContractSide.NO)


# -- Run all tests -----------------------------------------------------------

async def run_all():
    print("=" * 60)
    print("  Prediction Market Bot -- Test Suite (V2)")
    print("=" * 60)

    has_kalshi, has_openai = _run_config_checks()
    test_risk_manager()
    await test_kalshi(has_kalshi)
    await test_crypto_feed()
    test_latency_arb_logic()

    print("\n" + "=" * 60)
    passed = sum(1 for _, p in results if p is True)
    failed = sum(1 for _, p in results if p is False)
    print(f"  Results: {passed} passed, {failed} failed")
    print("=" * 60)
    return failed == 0

if __name__ == "__main__":
    success = asyncio.run(run_all())
    sys.exit(0 if success else 1)
