"""
Prediction Market Exploitation Bot -- Main Entry Point
Orchestrates all components: Kalshi integration, crypto feed,
strategies (V1 + V2), risk management, and execution.
Kalshi-only version (Polymarket unavailable in US).
"""

import asyncio
import logging
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.console import Group

from config import Config
from models import Platform
from exchanges.kalshi_wrapper import KalshiExchange
from data.crypto_feed import CryptoFeed
from data.aggregator import DataAggregator
from strategies.latency_arb import LatencyArbStrategy
from strategies.ai_market_scanner import AIMarketScanner
from strategies.linguistic_sniper import LinguisticSniperStrategy
from engine.risk_manager import RiskManager
from engine.executor import Executor
from engine.runtime_lock import acquire_single_instance_lock

# V2 Strategies
# V2 Strategies
from strategies_v2.yield_farmer import YieldFarmerStrategy
from strategies_v2.transcript_sniper import TranscriptSniperStrategy

console = Console()

# -- Logging setup -----------------------------------------------------------

def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(console=console, rich_tracebacks=True, markup=True),
            logging.FileHandler(log_dir / f"bot_{int(time.time())}.log"),
        ],
    )

    # Keep Live dashboard readable by suppressing per-request HTTP client INFO spam.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# -- Dashboard ---------------------------------------------------------------

def render_dashboard(
    risk_manager: RiskManager,
    executor: Executor,
    ai_scanner: AIMarketScanner,
    crypto_feed: CryptoFeed,
    yield_farmer: YieldFarmerStrategy,
    transcript: TranscriptSniperStrategy,
) -> Group:
    """Render a live dashboard table with bot stats."""
    risk_stats = risk_manager.get_stats()
    exec_stats = executor.get_stats()
    scanner_stats = ai_scanner.get_stats()
    yield_stats = yield_farmer.get_stats()
    transcript_stats = transcript.get_stats()

    table = Table(title="Prediction Market Bot -- Full Dashboard", expand=True)
    table.add_column("Metric", style="cyan", width=32)
    table.add_column("Value", style="green", width=28)

    # Portfolio
    table.add_row("--- Portfolio ---", "")
    table.add_row("Equity", f"${risk_stats['equity']:.2f}")
    table.add_row("P&L", f"${risk_stats['pnl']:.2f} ({risk_stats['pnl_pct']:+.2f}%)")
    table.add_row("Drawdown", f"{risk_stats['drawdown_pct']:.2f}%")
    table.add_row("Positions", str(risk_stats['positions']))
    table.add_row("Total Fees", f"${risk_stats['total_fees']:.4f}")
    table.add_row("Settlements Collected", f"${risk_stats.get('settlements_collected', 0.0):.2f}")
    table.add_row("Open Cost Basis", f"${risk_stats.get('open_notional_cost_basis', 0.0):.2f}")
    cb_status = "ACTIVE" if risk_stats['circuit_breaker_active'] else "OK"
    table.add_row("Circuit Breaker", cb_status)

    # Execution
    table.add_row("--- Execution ---", "")
    table.add_row("Signals Received", str(exec_stats['signals_received']))
    table.add_row("Signals Approved", str(exec_stats['signals_approved']))
    table.add_row("Fills Completed", str(exec_stats['fills_completed']))
    table.add_row("Fill Rate", f"{exec_stats['fill_rate']:.1f}%")

    # AI Scanner
    table.add_row("--- AI Market Scanner ---", "")
    table.add_row("Scanner Mode", str(scanner_stats.get('mode', 'unknown')))
    table.add_row("Markets Scanned", str(scanner_stats['markets_scanned']))
    table.add_row("Opportunities Found", str(scanner_stats['opportunities_found']))
    table.add_row("OpenAI Calls (1h)", str(scanner_stats.get('openai_calls_last_hour', 0)))
    table.add_row("OpenAI Tokens (Today)", str(scanner_stats.get('openai_tokens_today', 0)))
    table.add_row("OpenAI Token Budget", str(scanner_stats.get('openai_daily_token_budget', 0)))

    # V2 Strategies
    table.add_row("--- Certainty Yield Farmer ---", "")
    table.add_row("Active Markets", str(yield_stats.get('active_markets', 0)))
    table.add_row("Yields Captured", str(yield_stats.get('opportunities_found', 0)))

    table.add_row("--- Transcript Sniper ---", "")
    table.add_row("Targets Monitored", str(transcript_stats['targets']))
    table.add_row("Keywords Detected", str(transcript_stats['keywords_detected']))

    # Spot prices
    table.add_row("--- Spot Prices ---", "")
    btc = crypto_feed.get_latest_price("btcusdt")
    eth = crypto_feed.get_latest_price("ethusdt")
    table.add_row("BTC/USDT", f"${btc:,.2f}" if btc else "Connecting...")
    table.add_row("ETH/USDT", f"${eth:,.2f}" if eth else "Connecting...")

    # Mode
    mode = "PAPER TRADING" if Config.PAPER_TRADING else "LIVE TRADING"
    table.add_row("--- Mode ---", mode)

    # Active Positions Table
    pos_table = Table(title="Active Market Positions", expand=True)
    pos_table.add_column("Market Ticker", style="cyan")
    pos_table.add_column("Contract", style="yellow")
    pos_table.add_column("Size", style="green", justify="right")
    pos_table.add_column("Avg Price", style="magenta", justify="right")
    
    positions = risk_stats.get("active_positions", [])
    if not positions:
        pos_table.add_row("No active positions", "-", "-", "-")
    else:
        for p in positions:
            pos_table.add_row(
                p["market_id"],
                p["contract_side"].upper(),
                str(p["quantity"]),
                f"${p['avg_price']:.2f}"
            )

    return Group(table, pos_table)


# -- Main --------------------------------------------------------------------

async def main():
    setup_logging()

    try:
        acquire_single_instance_lock(
            "main",
            takeover_existing=Config.SESSION_TAKEOVER_EXISTING,
        )
    except RuntimeError as e:
        console.print(f"[red]{e}[/red]")
        return

    # -- Banner --
    console.print(Panel.fit(
        "[bold cyan]Prediction Market Exploitation Bot[/bold cyan]\n"
        "[dim]Kalshi-Only | Institutional Edge Edition[/dim]\n"
        "[dim]Yield Farmer | AI Scanner | Transcript Sniper[/dim]",
        border_style="bright_blue",
    ))

    # -- Config validation --
    warnings = Config.validate()
    if warnings:
        console.print("[yellow]Configuration warnings:[/yellow]")
        for w in warnings:
            console.print(f"  [yellow]* {w}[/yellow]")
        if not Config.PAPER_TRADING:
            console.print("[red]Cannot run in LIVE mode with missing credentials![/red]")
            return
        console.print("[green]Running in PAPER TRADING mode -- missing credentials are OK.[/green]\n")

    # -- Initialize components --
    kalshi = KalshiExchange()
    crypto_feed = CryptoFeed(
        symbols=["btcusdt", "ethusdt"],
        jump_threshold=Config.SPOT_PRICE_JUMP_THRESHOLD,
    )
    aggregator = DataAggregator(kalshi, crypto_feed)
    risk_manager = RiskManager(
        initial_equity=1000.0,
        state_file="logs/portfolio_main.json",
        session_name="main",
    )
    executor = Executor(kalshi, risk_manager)

    # V1 Strategies
    latency_arb = LatencyArbStrategy(aggregator)
    ai_scanner = AIMarketScanner(
        kalshi,
        pessimistic_mode=Config.SCANNER_PESSIMISTIC_MODE,
    )
    linguistic_sniper = LinguisticSniperStrategy()

    # V2 Strategies
    # V2 Strategies
    yield_farmer = YieldFarmerStrategy(kalshi=kalshi)
    transcript = TranscriptSniperStrategy(
        kalshi_exchange=kalshi,
        openai_api_key=Config.OPENAI_API_KEY,
        enable_whisper=Config.TRANSCRIPT_ENABLE_WHISPER,
    )

    # -- Connect to Kalshi --
    try:
        await kalshi.connect()
    except Exception as e:
        logger.warning("Kalshi connection failed (paper mode OK): %s", e)

    # -- Configure latency arb market mappings --
    latency_arb.configure_markets({
        "BTCUSDT": { "series": "KXBTC15M" },
        "ETHUSDT": { "series": "KXETH15M" },
    })

    # -- Wire up signal routing --
    original_latency_on_spot = latency_arb._on_spot_update

    async def latency_signal_router(update, delta):
        signals = await original_latency_on_spot(update, delta)
        if signals:
            for s in signals:
                await executor.submit_signal(s)

    latency_arb._on_spot_update = latency_signal_router

    # -- Start everything --
    await executor.start()
    
    # We explicitly disable HFT latency strategies due to adverse selection
    # await latency_arb.start()
    # await linguistic_sniper.start()

    await ai_scanner.start()
    await yield_farmer.start()
    await transcript.start()

    # Discover mention markets for transcript sniper
    try:
        await transcript.discover_mention_markets()
    except Exception as e:
        logger.warning("Transcript market discovery: %s", e)

    tasks = []

    # Crypto spot feed
    tasks.append(asyncio.create_task(crypto_feed.start()))

    # AI market scanner loop
    tasks.append(asyncio.create_task(
        ai_scanner.run_scan_loop(signal_callback=executor.submit_signal)
    ))

    # Yield farming loop
    tasks.append(asyncio.create_task(
        yield_farmer.run_loop(signal_callback=executor.submit_signal)
    ))

    console.print("[green]Institutional strategies running: AI Scanner, Yield Farmer, Transcript Sniper[/green]\n")

    async def portfolio_monitor_loop():
        """Monitor open positions and settle resolved markets."""
        while True:
            try:
                if Config.PAPER_TRADING and len(risk_manager._positions) > 0:
                    # In Paper Trading, check if active markets have settled
                    # _positions keys are now (market_id, ContractSide) tuples.
                    # We extract unique market_ids so we don't fetch the same market multiple times.
                    unique_markets = set(m_id for m_id, _ in risk_manager._positions.keys())
                    
                    for ticker in unique_markets:
                        try:
                            m_detail = await kalshi.get_market(ticker)
                            if not m_detail:
                                logger.warning("Portfolio: Market %s not found on Kalshi (404), keeping in limbo.", ticker)
                                continue

                            status = str(m_detail.get("status", "")).lower()
                            result = str(m_detail.get("result", "")).lower()
                            settlement_ts = m_detail.get("settlement_ts")
                            settlement_value = m_detail.get("settlement_value_dollars")

                            # Prefer explicit settlement_value_dollars when present.
                            yes_contract_payout = None
                            if settlement_value not in (None, ""):
                                try:
                                    yes_contract_payout = float(settlement_value)
                                except (TypeError, ValueError):
                                    yes_contract_payout = None

                            # Fallback to binary outcome mapping when settlement value is absent.
                            if yes_contract_payout is None and result in ["yes", "no"]:
                                yes_contract_payout = 1.0 if result == "yes" else 0.0

                            # Kalshi can mark resolved contracts as finalized/determined before full settlement.
                            if yes_contract_payout is not None and (
                                status in {"settled", "finalized", "determined"}
                                or settlement_ts
                            ):
                                logger.info(
                                    "Portfolio: %s resolved (status=%s, result=%s, payout=%.4f).",
                                    ticker,
                                    status or "unknown",
                                    result or "n/a",
                                    yes_contract_payout,
                                )
                                risk_manager.record_settlement(ticker, yes_contract_payout)
                        except Exception as e:
                            logger.error("Error checking position %s: %s", ticker, e)
                            
                await asyncio.sleep(60) # check every minute
            except Exception as e:
                logger.error("Portfolio monitor error: %s", e)
                await asyncio.sleep(10)

    # -- Live dashboard loop --
    tasks.append(asyncio.create_task(portfolio_monitor_loop()))

    try:
        console.clear()
        with Live(
            render_dashboard(risk_manager, executor, ai_scanner, crypto_feed,
                           yield_farmer, transcript),
            refresh_per_second=1,
            console=console,
            transient=False,
        ) as live:
            while True:
                try:
                    dash = render_dashboard(
                        risk_manager, executor, ai_scanner, crypto_feed,
                        yield_farmer, transcript
                    )
                    live.update(dash)
                except Exception:
                    pass
                await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.print("\n[yellow]Shutting down...[/yellow]")
    except Exception as e:
        logger.error("Dashboard error: %s", e)
    finally:
        # -- Cleanup --
        await latency_arb.stop()
        await ai_scanner.stop()
        await linguistic_sniper.stop()
        await yield_farmer.stop()
        await transcript.stop()
        await executor.stop()
        await crypto_feed.stop()
        await kalshi.disconnect()

        for task in tasks:
            if not task.done():
                task.cancel()

    # Final stats
    console.print("\n[bold cyan]Final Stats:[/bold cyan]")
    risk_stats = risk_manager.get_stats()
    console.print(f"  Equity: ${risk_stats['equity']:.2f}")
    console.print(f"  P&L: ${risk_stats['pnl']:.2f} ({risk_stats['pnl_pct']:+.2f}%)")
    console.print(f"  Total Trades: {risk_stats['total_trades']}")
    console.print(f"  Total Fees: ${risk_stats['total_fees']:.4f}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
