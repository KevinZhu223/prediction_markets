"""
Risk Manager
Implements Quarter-Kelly sizing, position limits, sector caps,
drawdown circuit breakers, and toxic flow detection.

Core idea from research:
  "A profitable bot is one that survives. Systematic edges in prediction
   markets are often thin (0.5%-2% per trade), meaning a single mismanaged
   loss can wipe out weeks of gains."
"""

import logging
import time
import os
import json
from typing import Optional

from config import Config
from models import Signal, Position, Fill, Platform, Side, ContractSide

logger = logging.getLogger(__name__)


class RiskManager:
    """
    Gatekeeper that validates every signal before execution.
    Enforces portfolio constraints and dynamic risk limits.
    """

    def __init__(self, initial_equity: float = 1000.0, state_file: str = "portfolio.json"):
        self.state_file = state_file
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.peak_equity = initial_equity

        # Active positions: dict[(market_id, contract_side), Position]
        self._positions: dict[tuple[str, ContractSide], Position] = {}

        # Trade history for P&L tracking
        self._fills: list[Fill] = []
        self._settlements_collected: float = 0.0

        # Sector exposure tracking
        self._sector_exposure: dict[str, float] = {}

        # Drawdown tracking
        self._max_drawdown_hit = False

        # Volume spike detection for toxic flow
        self._recent_volumes: dict[str, list[tuple[float, int]]] = {}

        # Pending signals currently in flight to the executor
        self._pending_signals: dict[tuple[str, ContractSide], int] = {}
        
        self.load_state()

    def load_state(self):
        """Loads equity and open positions from the state JSON file if it exists."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                    
                self.initial_equity = data.get("initial_equity", self.initial_equity)
                self.current_equity = data.get("current_equity", self.initial_equity)
                self.peak_equity = data.get("peak_equity", self.initial_equity)
                
                positions_data = data.get("positions", [])
                for p_data in positions_data:
                    try:
                        contract_side = ContractSide(p_data["contract_side"])
                        pos_key = (p_data["market_id"], contract_side)
                        self._positions[pos_key] = Position(
                            market_id=p_data["market_id"],
                            platform=Platform(p_data.get("platform", "kalshi")),
                            side=Side(p_data.get("side", "buy")),
                            contract_side=contract_side,
                            quantity=p_data["quantity"],
                            avg_price=p_data["avg_price"]
                        )
                    except Exception as e:
                        logger.error("Error loading position %s: %s", p_data.get("market_id"), e)
                        
                logger.info("Loaded portfolio state from %s (Equity: $%.2f)", self.state_file, self.current_equity)
            except Exception as e:
                logger.error("Failed to load portfolio state: %s", e)

    def save_state(self):
        """Saves current equity and open positions to the state JSON file."""
        try:
            positions_data = []
            for pos in self._positions.values():
                positions_data.append({
                    "market_id": pos.market_id,
                    "platform": pos.platform.value,
                    "side": pos.side.value,
                    "contract_side": pos.contract_side.value,
                    "quantity": pos.quantity,
                    "avg_price": pos.avg_price
                })
            
            data = {
                "initial_equity": self.initial_equity,
                "current_equity": self.current_equity,
                "peak_equity": self.peak_equity,
                "positions": positions_data
            }
            with open(self.state_file, "w") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error("Failed to save portfolio state: %s", e)

    # ── Signal validation ───────────────────────────────────────────────

    def validate_signal(self, signal: Signal) -> tuple[bool, str, int]:
        """
        Validate whether a signal should be executed.
        Returns: (approved, reason, adjusted_quantity)
        """
        # Circuit breaker: max drawdown
        if self._max_drawdown_hit:
            return False, "Circuit breaker active: max drawdown exceeded", 0

        drawdown = self._current_drawdown()
        if drawdown >= Config.MAX_DRAWDOWN_PCT:
            self._max_drawdown_hit = True
            logger.critical(
                "CIRCUIT BREAKER: Drawdown %.2f%% >= %.2f%% limit",
                drawdown * 100, Config.MAX_DRAWDOWN_PCT * 100,
            )
            return False, f"Max drawdown {drawdown:.2%} exceeded", 0

        # Minimum confidence threshold (Lowered to 0.30 to see more activity)
        if signal.confidence < 0.30:
            return False, f"Confidence {signal.confidence:.2f} below 0.30 threshold", 0

        # Position size check
        trade_value = signal.target_price * signal.quantity
        max_position_value = self.current_equity * Config.MAX_POSITION_PCT
        adjusted_qty = signal.quantity

        if trade_value > max_position_value:
            adjusted_qty = max(1, int(max_position_value / signal.target_price))
            logger.info(
                "Risk: sizing %s from %d to %d (max position %.2f%%)",
                signal.market_id, signal.quantity, adjusted_qty,
                Config.MAX_POSITION_PCT * 100,
            )

        # Kelly Criterion sizing
        kelly_qty = self._kelly_size(signal)
        if kelly_qty < adjusted_qty:
            adjusted_qty = kelly_qty
            logger.info(
                "Risk: Kelly sizing %s to %d contracts",
                signal.market_id, adjusted_qty,
            )

        if adjusted_qty <= 0:
            return False, "Kelly sizing resulted in zero quantity", 0

        # Check for existing + pending position concentration
        pos_key = (signal.market_id, signal.contract_side)
        current_pos = self._positions.get(pos_key)
        current_qty = current_pos.quantity if current_pos else 0
        
        # Check if we have any signals in flight for this ticker
        pending_qty = self._pending_signals.get(pos_key, 0)
        
        total_projected_qty = current_qty + pending_qty + adjusted_qty
        projected_value = total_projected_qty * signal.target_price
        
        if projected_value > max_position_value:
            # Re-calculate how much room we actually have
            remaining_value = max_position_value - ((current_qty + pending_qty) * signal.target_price)
            if remaining_value <= 0:
                return False, "Position limit already reached (including pending)", 0
            
            new_adj_qty = max(0, int(remaining_value / signal.target_price))
            if new_adj_qty < adjusted_qty:
                adjusted_qty = new_adj_qty
                logger.info("Risk: capped %s at %d due to projected concentration", signal.market_id, adjusted_qty)

        if adjusted_qty <= 0:
            return False, "Position limit reached", 0

        # Mark as pending so we don't over-trade in the next millisecond
        self._pending_signals[pos_key] = pending_qty + adjusted_qty
        
        return True, "Approved", adjusted_qty

    # ── Kelly Criterion ─────────────────────────────────────────────────

    def _kelly_size(self, signal: Signal) -> int:
        """
        Quarter-Kelly sizing.
        f* = (p * b - q) / b
        where p = probability of winning, b = odds, q = 1 - p
        We use signal.confidence as estimated probability.
        """
        p = signal.confidence
        q = 1.0 - p

        # For binary contracts: if we buy at `price`, we win (1 - price) if correct
        buy_price = signal.target_price
        if buy_price <= 0 or buy_price >= 1:
            return 0

        # Odds = (1 - price) / price
        b = (1.0 - buy_price) / buy_price
        if b <= 0:
            return 0

        # Full Kelly fraction
        full_kelly = (p * b - q) / b
        if full_kelly <= 0:
            return 0

        # Quarter Kelly
        quarter_kelly = full_kelly * Config.KELLY_FRACTION

        # Convert to contract count
        kelly_value = self.current_equity * quarter_kelly
        contracts = int(kelly_value / buy_price)

        # Round up if the edge is substantial (>10%) to ensure we at least buy 1 contract
        if contracts <= 0 and (p - buy_price) >= 0.10:
            contracts = 1
            
        return max(0, contracts)

    # ── Drawdown tracking ───────────────────────────────────────────────

    def _current_drawdown(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return (self.peak_equity - self.current_equity) / self.peak_equity

    # ── Position management ─────────────────────────────────────────────

    def record_fill(self, fill: Fill):
        """Record a completed trade and update portfolio state."""
        self._fills.append(fill)
        key = fill.order.market_id
        pos_key = (fill.order.market_id, fill.order.contract_side)

        # Clear pending count for this market
        if pos_key in self._pending_signals:
            self._pending_signals[pos_key] = max(0, self._pending_signals[pos_key] - fill.fill_quantity)
            if self._pending_signals[pos_key] <= 0:
                del self._pending_signals[pos_key]

        if fill.order.side == Side.BUY:
            if pos_key in self._positions:
                pos = self._positions[pos_key]
                total_qty = pos.quantity + fill.fill_quantity
                pos.avg_price = (
                    (pos.avg_price * pos.quantity + fill.fill_price * fill.fill_quantity)
                    / total_qty
                )
                pos.quantity = total_qty
            else:
                self._positions[pos_key] = Position(
                    market_id=key,
                    platform=fill.order.platform,
                    side=Side.BUY,
                    contract_side=fill.order.contract_side,
                    quantity=fill.fill_quantity,
                    avg_price=fill.fill_price,
                )
            # Deduct from equity
            self.current_equity -= (fill.fill_price * fill.fill_quantity + fill.fees)
        else:
            # SELL: add proceeds
            self.current_equity += (fill.fill_price * fill.fill_quantity - fill.fees)
            if pos_key in self._positions:
                self._positions[pos_key].quantity -= fill.fill_quantity
                if self._positions[pos_key].quantity <= 0:
                    del self._positions[pos_key]

        # Update peak
        self.peak_equity = max(self.peak_equity, self.current_equity)

        logger.info(
            "Risk: equity=$%.2f (peak=$%.2f, dd=%.2f%%), positions=%d",
            self.current_equity, self.peak_equity,
            self._current_drawdown() * 100, len(self._positions),
        )
        self.save_state()

    def record_settlement(self, market_id: str, payout_per_contract: float):
        """Record a market settlement (contract resolved)."""
        # Settle YES
        yes_key = (market_id, ContractSide.YES)
        if yes_key in self._positions:
            pos = self._positions[yes_key]
            payout = payout_per_contract * pos.quantity
            self.current_equity += payout
            self._settlements_collected += payout
            self.peak_equity = max(self.peak_equity, self.current_equity)
            del self._positions[yes_key]
            logger.info("Settlement: %s (YES) paid $%.2f", market_id, payout)
            
        # Settle NO
        no_key = (market_id, ContractSide.NO)
        if no_key in self._positions:
            pos = self._positions[no_key]
            # If YES pays out 1.0, NO pays out 0.0. If YES pays 0.0, NO pays 1.0.
            no_payout_per_contract = 1.0 - payout_per_contract
            payout = no_payout_per_contract * pos.quantity
            self.current_equity += payout
            self._settlements_collected += payout
            self.peak_equity = max(self.peak_equity, self.current_equity)
            del self._positions[no_key]
            logger.info("Settlement: %s (NO) paid $%.2f", market_id, payout)

        self.save_state()


    def record_failure(
        self,
        market_id: str,
        quantity: int,
        contract_side: Optional[ContractSide] = None,
    ):
        """Release pending allocation for a failed order."""
        if contract_side is not None:
            key = (market_id, contract_side)
            if key in self._pending_signals:
                self._pending_signals[key] = max(0, self._pending_signals[key] - quantity)
                if self._pending_signals[key] <= 0:
                    del self._pending_signals[key]
            return

        # Fallback for legacy call sites: release all pending entries for this market.
        keys = [k for k in self._pending_signals.keys() if k[0] == market_id]
        for key in keys:
            self._pending_signals[key] = max(0, self._pending_signals[key] - quantity)
            if self._pending_signals[key] <= 0:
                del self._pending_signals[key]

    # ── Toxic flow detection ────────────────────────────────────────────

    def detect_toxic_flow(self, market_id: str, volume: int) -> bool:
        """
        Detect sudden volume spikes that may indicate insider activity.
        Returns True if the volume spike is suspicious.
        """
        now = time.time()

        if market_id not in self._recent_volumes:
            self._recent_volumes[market_id] = []

        history = self._recent_volumes[market_id]
        history.append((now, volume))

        # Keep last 60 seconds
        history[:] = [(t, v) for t, v in history if now - t < 60]

        if len(history) < 3:
            return False

        avg_volume = sum(v for _, v in history[:-1]) / (len(history) - 1)
        if avg_volume <= 0:
            return False

        # If the latest volume is 5x the average, flag it
        spike_ratio = volume / avg_volume
        if spike_ratio >= 5.0:
            logger.warning(
                "TOXIC FLOW detected on %s: volume=%d, avg=%d, ratio=%.1fx",
                market_id, volume, avg_volume, spike_ratio,
            )
            return True

        return False

    def reset_circuit_breaker(self):
        """Manual reset after reviewing the drawdown situation."""
        self._max_drawdown_hit = False
        logger.info("Circuit breaker manually reset")

    # ── Stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        total_fees = sum(f.fees for f in self._fills)
        total_trades = len(self._fills)
        active_positions = [
            {
                "market_id": pos.market_id,
                "contract_side": pos.contract_side.value,
                "quantity": pos.quantity,
                "avg_price": pos.avg_price
            }
            for pos in self._positions.values()
        ]
        return {
            "equity": self.current_equity,
            "initial_equity": self.initial_equity,
            "peak_equity": self.peak_equity,
            "drawdown_pct": self._current_drawdown() * 100,
            "circuit_breaker_active": self._max_drawdown_hit,
            "positions": len(self._positions),
            "active_positions": active_positions,
            "total_trades": total_trades,
            "total_fees": total_fees,
            "pnl": self.current_equity - self.initial_equity,
            "pnl_pct": ((self.current_equity / self.initial_equity) - 1) * 100
            if self.initial_equity > 0 else 0,
        }
