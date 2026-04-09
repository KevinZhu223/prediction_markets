"""
Executor
Takes validated signals from the risk manager and routes them
to Kalshi for execution.
Handles Fill-or-Kill logic, paper trading mode, and trade logging.
"""

import asyncio
import logging
import time
from typing import Optional

from config import Config
from models import Signal, Order, Fill, OrderType, Side, Platform
from exchanges.base import ExchangeBase
from engine.risk_manager import RiskManager

logger = logging.getLogger(__name__)


class Executor:
    """
    Routes signals to Kalshi after risk validation.
    In paper trading mode, simulates fills and tracks performance.
    """

    def __init__(
        self,
        kalshi: ExchangeBase,
        risk_manager: RiskManager,
        kalshi_standard: ExchangeBase = None,
    ):
        self._exchange = kalshi
        self._exchange_standard = kalshi_standard
        self.risk_manager = risk_manager

        self._queue_max_size = max(1, Config.EXECUTOR_QUEUE_MAX_SIZE)
        self._queue_warn_threshold = max(1, Config.EXECUTOR_QUEUE_WARN_THRESHOLD)
        self._drop_when_full = Config.EXECUTOR_DROP_WHEN_FULL

        # Signal queue for batch processing
        self._signal_queue: asyncio.Queue = asyncio.Queue(maxsize=self._queue_max_size)
        self._active = False

        # Execution stats
        self._signals_received = 0
        self._signals_approved = 0
        self._signals_rejected = 0
        self._fills_completed = 0
        self._fills_failed = 0
        self._signals_dropped_overload = 0
        self._signals_dropped_stale = 0
        self._signals_dropped_duplicate = 0

        self._duplicate_cooldown_seconds = max(0.0, Config.SIGNAL_DUPLICATE_COOLDOWN_SECONDS)
        self._duplicate_price_tolerance = max(0.0, Config.SIGNAL_DUPLICATE_PRICE_TOLERANCE)
        self._recent_signals: dict[tuple[str, str, str], tuple[float, float]] = {}
        self._strategy_stats: dict[str, dict[str, int]] = {}

    def _get_strategy_stats(self, strategy: str) -> dict[str, int]:
        return self._strategy_stats.setdefault(
            strategy,
            {
                "received": 0,
                "approved": 0,
                "rejected": 0,
                "fills_completed": 0,
                "fills_failed": 0,
                "dropped_overload": 0,
                "dropped_stale": 0,
                "dropped_duplicate": 0,
            },
        )

    async def start(self):
        """Start the execution loop."""
        self._active = True
        logger.info(
            "Executor: started (paper=%s)", Config.PAPER_TRADING
        )
        asyncio.create_task(self._process_loop())

    async def stop(self):
        self._active = False

    async def submit_signal(self, signal: Signal):
        """Submit a signal for risk check and execution."""
        if self._is_duplicate_signal(signal):
            self._signals_dropped_duplicate += 1
            self._signals_rejected += 1
            self._get_strategy_stats(signal.strategy)["dropped_duplicate"] += 1
            logger.info(
                "Executor duplicate suppression: dropped %s %s %s @ %.4f",
                signal.strategy,
                signal.market_id,
                signal.contract_side.value,
                signal.target_price,
            )
            return

        queue_size = self._signal_queue.qsize()

        if queue_size >= self._queue_max_size and self._drop_when_full:
            self._signals_dropped_overload += 1
            self._signals_rejected += 1
            self._get_strategy_stats(signal.strategy)["dropped_overload"] += 1
            logger.warning(
                "Executor overload: dropping signal %s (%s) queue=%d/%d",
                signal.market_id,
                signal.strategy,
                queue_size,
                self._queue_max_size,
            )
            return

        if queue_size >= self._queue_warn_threshold:
            logger.warning(
                "Executor queue pressure: %d pending (warn=%d, max=%d)",
                queue_size,
                self._queue_warn_threshold,
                self._queue_max_size,
            )

        await self._signal_queue.put(signal)

    async def submit_signals(self, signals: list[Signal]):
        """Submit multiple signals."""
        for s in signals:
            await self.submit_signal(s)

    async def _process_loop(self):
        """Main loop: pull signals from queue, validate, execute."""
        while self._active:
            try:
                signal = await asyncio.wait_for(
                    self._signal_queue.get(), timeout=0.5
                )
                await self._process_signal(signal)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Executor loop error: %s", e)

    async def _process_signal(self, signal: Signal):
        """Process a single signal through risk check -> execution."""
        self._signals_received += 1
        strategy_stats = self._get_strategy_stats(signal.strategy)
        strategy_stats["received"] += 1

        signal_age = max(0.0, time.time() - signal.timestamp)
        max_signal_age = max(1.0, Config.SIGNAL_MAX_AGE_SECONDS)
        if signal_age > max_signal_age:
            self._signals_dropped_stale += 1
            self._signals_rejected += 1
            strategy_stats["rejected"] += 1
            strategy_stats["dropped_stale"] += 1
            logger.info(
                "X Signal REJECTED: %s %s on %s -- stale signal (age=%.2fs > %.2fs)",
                signal.action.value,
                signal.market_id,
                signal.platform.value,
                signal_age,
                max_signal_age,
            )
            return

        # Risk validation
        approved, reason, adjusted_qty = self.risk_manager.validate_signal(signal)

        if not approved:
            self._signals_rejected += 1
            strategy_stats["rejected"] += 1
            logger.info(
                "X Signal REJECTED: %s %s on %s -- %s",
                signal.action.value, signal.market_id, signal.platform.value, reason,
            )
            return

        self._signals_approved += 1
        strategy_stats["approved"] += 1

        # Build the order
        order = Order(
            market_id=signal.market_id,
            platform=signal.platform,
            side=signal.action,
            contract_side=signal.contract_side,
            price=signal.target_price,
            quantity=adjusted_qty,
            order_type=OrderType.FOK,
        )

        # Price Protection Check: 
        # For BUY orders, ensure we don't pay more than the AI's fair value.
        # The signal reason contains "fair=55.0%" (percentage format from :.1%)
        if "fair=" in signal.reason:
            try:
                import re
                match = re.search(r'fair=([\d.]+)%', signal.reason)
                if match:
                    fair_val = float(match.group(1)) / 100.0  # Convert 55.0% -> 0.55
                    # If we are BUYING YES at a price > fair_value, it's a negative edge trade.
                    if signal.action == Side.BUY and signal.target_price > fair_val:
                        self._signals_rejected += 1
                        strategy_stats["rejected"] += 1
                        self.risk_manager.record_failure(
                            signal.market_id,
                            adjusted_qty,
                            signal.contract_side,
                        )
                        logger.warning(
                            "PRICE PROTECTION: aborting buy %s @ %.2f (fair=%.2f) -- would overpay!",
                            signal.market_id, signal.target_price, fair_val
                        )
                        return
            except Exception as e:
                logger.error("Price protection check failed: %s", e)

        logger.info(
            "Executing: %s %s %d x %s @ %.4f on %s [%s]",
            order.side.value,
            order.contract_side.value,
            order.quantity,
            order.market_id,
            order.price, order.platform.value,
            "PAPER" if Config.PAPER_TRADING else "LIVE",
        )

        try:
            exchange = self._exchange_standard if order.platform == Platform.KALSHI_STANDARD and self._exchange_standard else self._exchange
            fill = await exchange.place_order(order)

            if fill:
                self._fills_completed += 1
                strategy_stats["fills_completed"] += 1
                self.risk_manager.record_fill(fill)
                logger.info(
                    "FILLED: %s %s %d x %s @ %.4f (fees=$%.4f)",
                    fill.order.side.value,
                    fill.order.contract_side.value,
                    fill.fill_quantity,
                    fill.order.market_id,
                    fill.fill_price,
                    fill.fees,
                )
            else:
                self._fills_failed += 1
                strategy_stats["fills_failed"] += 1
                self.risk_manager.record_failure(order.market_id, order.quantity, order.contract_side)
                logger.warning(
                    "Order not filled (FOK miss): %s %s %s @ %.4f",
                    order.side.value,
                    order.contract_side.value,
                    order.market_id,
                    order.price,
                )

        except Exception as e:
            self._fills_failed += 1
            strategy_stats["fills_failed"] += 1
            self.risk_manager.record_failure(order.market_id, order.quantity, order.contract_side)
            logger.error("Execution error: %s", e)

    def _is_duplicate_signal(self, signal: Signal) -> bool:
        if self._duplicate_cooldown_seconds <= 0:
            return False

        now = time.time()
        if len(self._recent_signals) > 2000:
            cutoff = now - self._duplicate_cooldown_seconds
            self._recent_signals = {
                k: v for k, v in self._recent_signals.items()
                if v[0] >= cutoff
            }

        key = (signal.strategy, signal.market_id, signal.contract_side.value)
        last = self._recent_signals.get(key)
        if last:
            last_ts, last_price = last
            if (
                (now - last_ts) <= self._duplicate_cooldown_seconds
                and abs(float(signal.target_price) - last_price) <= self._duplicate_price_tolerance
            ):
                return True

        self._recent_signals[key] = (now, float(signal.target_price))
        return False

    # ── Stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        return {
            "signals_received": self._signals_received,
            "signals_approved": self._signals_approved,
            "signals_rejected": self._signals_rejected,
            "fills_completed": self._fills_completed,
            "fills_failed": self._fills_failed,
            "signals_dropped_overload": self._signals_dropped_overload,
            "signals_dropped_stale": self._signals_dropped_stale,
            "signals_dropped_duplicate": self._signals_dropped_duplicate,
            "fill_rate": (
                self._fills_completed / self._signals_approved * 100
                if self._signals_approved > 0 else 0
            ),
            "queue_size": self._signal_queue.qsize(),
            "queue_max_size": self._queue_max_size,
            "strategy_stats": dict(self._strategy_stats),
        }
