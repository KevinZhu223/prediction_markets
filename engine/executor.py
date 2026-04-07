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

        # Signal queue for batch processing
        self._signal_queue: asyncio.Queue = asyncio.Queue()
        self._active = False

        # Execution stats
        self._signals_received = 0
        self._signals_approved = 0
        self._signals_rejected = 0
        self._fills_completed = 0
        self._fills_failed = 0

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
        await self._signal_queue.put(signal)

    async def submit_signals(self, signals: list[Signal]):
        """Submit multiple signals."""
        for s in signals:
            await self._signal_queue.put(s)

    async def _process_loop(self):
        """Main loop: pull signals from queue, validate, execute."""
        while self._active:
            try:
                signal = await asyncio.wait_for(
                    self._signal_queue.get(), timeout=1.0
                )
                await self._process_signal(signal)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Executor loop error: %s", e)

    async def _process_signal(self, signal: Signal):
        """Process a single signal through risk check -> execution."""
        self._signals_received += 1

        # Risk validation
        approved, reason, adjusted_qty = self.risk_manager.validate_signal(signal)

        if not approved:
            self._signals_rejected += 1
            logger.info(
                "X Signal REJECTED: %s %s on %s -- %s",
                signal.action.value, signal.market_id, signal.platform.value, reason,
            )
            return

        self._signals_approved += 1

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
            self.risk_manager.record_failure(order.market_id, order.quantity, order.contract_side)
            logger.error("Execution error: %s", e)

    # ── Stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        return {
            "signals_received": self._signals_received,
            "signals_approved": self._signals_approved,
            "signals_rejected": self._signals_rejected,
            "fills_completed": self._fills_completed,
            "fills_failed": self._fills_failed,
            "fill_rate": (
                self._fills_completed / self._signals_approved * 100
                if self._signals_approved > 0 else 0
            ),
            "queue_size": self._signal_queue.qsize(),
        }
