"""
Risk Manager
Implements Quarter-Kelly sizing, position limits, sector caps,
drawdown circuit breakers, and toxic flow detection.

Core idea from research:
  "A profitable bot is one that survives. Systematic edges in prediction
   markets are often thin (0.5%-2% per trade), meaning a single mismanaged
   loss can wipe out weeks of gains."
"""

from __future__ import annotations

import logging
import os
import json
import time
from pathlib import Path
from typing import Optional, Any

from config import Config
from models import Signal, Position, Fill, Platform, Side, ContractSide

logger = logging.getLogger(__name__)


class RiskManager:
    """
    Gatekeeper that validates every signal before execution.
    Enforces portfolio constraints and dynamic risk limits.
    """

    def __init__(
        self,
        initial_equity: float = 1000.0,
        state_file: str = "portfolio.json",
        session_name: str = "default",
    ):
        self.session_name = session_name
        self.state_file = state_file
        self._state_path = Path(state_file)
        self.initial_equity = initial_equity
        self.current_equity = initial_equity
        self.peak_equity = initial_equity
        self._session_started_at = time.time()

        # Effective risk limits. In paper testing, we can relax limits to evaluate strategy signal quality faster.
        self.min_signal_confidence = Config.MIN_SIGNAL_CONFIDENCE
        self.max_position_pct = Config.MAX_POSITION_PCT
        self.max_drawdown_pct = Config.MAX_DRAWDOWN_PCT
        self.kelly_fraction = Config.KELLY_FRACTION
        self.risk_profile = "base"

        if Config.PAPER_TRADING and Config.RISK_TEST_MODE:
            self.max_position_pct = max(self.max_position_pct, Config.TEST_MAX_POSITION_PCT)
            self.max_drawdown_pct = max(self.max_drawdown_pct, Config.TEST_MAX_DRAWDOWN_PCT)
            self.kelly_fraction = max(self.kelly_fraction, Config.TEST_KELLY_FRACTION)
            self.risk_profile = "testing-relaxed"

        # Active positions: dict[(market_id, contract_side), Position]
        self._positions: dict[tuple[str, ContractSide], Position] = {}

        # Trade history for P&L tracking
        self._fills: list[Fill] = []
        self._settlements_collected: float = 0.0
        self._gross_buy_notional: float = 0.0
        self._gross_sell_notional: float = 0.0

        # Sector exposure tracking
        self._sector_exposure: dict[str, float] = {}

        # Drawdown tracking
        self._max_drawdown_hit = False

        # Volume spike detection for toxic flow
        self._recent_volumes: dict[str, list[tuple[float, int]]] = {}

        # Pending signals currently in flight to the executor
        self._pending_signals: dict[tuple[str, ContractSide], int] = {}
        self._pending_updated_at: dict[tuple[str, ContractSide], float] = {}
        self._pending_signal_ttl_seconds = max(5.0, Config.PENDING_SIGNAL_TTL_SECONDS)
        self._pending_cleanup_last = 0.0

        # Runtime telemetry files for VM monitoring.
        self._journal_enabled = Config.TRADE_JOURNAL_ENABLED
        self._journal_path: Optional[Path] = None
        self._summary_path: Optional[Path] = None

        self._ensure_state_parent()
        self.load_state()
        self._init_journal_files()
        self._write_journal_event(
            "session_start",
            {
                "state_file": str(self._state_path),
                "risk_profile": self.risk_profile,
                "limits": {
                    "min_signal_confidence": self.min_signal_confidence,
                    "max_position_pct": self.max_position_pct,
                    "max_drawdown_pct": self.max_drawdown_pct,
                    "kelly_fraction": self.kelly_fraction,
                },
            },
        )

    def _ensure_state_parent(self):
        parent = self._state_path.parent
        if str(parent) and str(parent) != ".":
            parent.mkdir(parents=True, exist_ok=True)

    def _init_journal_files(self):
        if not self._journal_enabled:
            return
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        run_id = int(time.time())
        self._journal_path = log_dir / f"trade_journal_{self.session_name}_{run_id}.jsonl"
        self._summary_path = log_dir / f"trade_summary_{self.session_name}.json"
        logger.info("Trade journal: %s", self._journal_path)
        self._write_summary_snapshot()

    def _write_summary_snapshot(self):
        if not self._summary_path:
            return
        try:
            payload = {
                "updated_at": time.time(),
                "session_name": self.session_name,
                **self.get_stats(),
            }
            with open(self._summary_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.error("Failed to write trade summary snapshot: %s", e)

    def _write_journal_event(self, event_type: str, payload: dict[str, Any]):
        if not self._journal_enabled or not self._journal_path:
            return
        try:
            event = {
                "ts": time.time(),
                "session_name": self.session_name,
                "event_type": event_type,
                "equity": self.current_equity,
                "peak_equity": self.peak_equity,
                "pnl": self.current_equity - self.initial_equity,
                "drawdown_pct": self._current_drawdown() * 100,
                "open_positions": len(self._positions),
                "payload": payload,
            }
            with open(self._journal_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
            self._write_summary_snapshot()
        except Exception as e:
            logger.error("Failed to write trade journal event: %s", e)

    def _position_cost_basis(self) -> float:
        return sum(pos.avg_price * pos.quantity for pos in self._positions.values())

    def load_state(self):
        """Loads equity and open positions from the state JSON file if it exists."""
        if self._state_path.exists():
            try:
                with open(self._state_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                self.initial_equity = data.get("initial_equity", self.initial_equity)
                self.current_equity = data.get("current_equity", self.initial_equity)
                self.peak_equity = data.get("peak_equity", self.initial_equity)
                self._settlements_collected = float(data.get("settlements_collected", 0.0) or 0.0)

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
                "settlements_collected": self._settlements_collected,
                "updated_at": time.time(),
                "positions": positions_data
            }
            with open(self._state_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error("Failed to save portfolio state: %s", e)

    # ── Signal validation ───────────────────────────────────────────────

    def _cleanup_stale_pending(self, force: bool = False):
        """Drop pending reservations that were never reconciled by fill/failure events."""
        now = time.time()
        if not force and (now - self._pending_cleanup_last) < 0.5:
            return
        self._pending_cleanup_last = now

        stale_keys: list[tuple[str, ContractSide]] = []
        for key, ts in list(self._pending_updated_at.items()):
            if (now - ts) > self._pending_signal_ttl_seconds:
                stale_keys.append(key)

        for key in stale_keys:
            qty = self._pending_signals.pop(key, 0)
            self._pending_updated_at.pop(key, None)
            if qty > 0:
                logger.warning(
                    "Risk: cleared stale pending allocation for %s %s (qty=%d, age>%.1fs)",
                    key[0],
                    key[1].value,
                    qty,
                    self._pending_signal_ttl_seconds,
                )

    def validate_signal(self, signal: Signal) -> tuple[bool, str, int]:
        """
        Validate whether a signal should be executed.
        Returns: (approved, reason, adjusted_quantity)
        """
        self._cleanup_stale_pending()

        # Circuit breaker: max drawdown
        if self._max_drawdown_hit:
            return False, "Circuit breaker active: max drawdown exceeded", 0

        drawdown = self._current_drawdown()
        if drawdown >= self.max_drawdown_pct:
            self._max_drawdown_hit = True
            logger.critical(
                "CIRCUIT BREAKER: Drawdown %.2f%% >= %.2f%% limit",
                drawdown * 100, self.max_drawdown_pct * 100,
            )
            return False, f"Max drawdown {drawdown:.2%} exceeded", 0

        # Minimum confidence threshold
        if signal.confidence < self.min_signal_confidence:
            return False, f"Confidence {signal.confidence:.2f} below {self.min_signal_confidence:.2f} threshold", 0

        # Position size check
        trade_value = signal.target_price * signal.quantity
        max_position_value = self.current_equity * self.max_position_pct
        adjusted_qty = signal.quantity

        if trade_value > max_position_value:
            adjusted_qty = max(1, int(max_position_value / signal.target_price))
            logger.info(
                "Risk: sizing %s from %d to %d (max position %.2f%%)",
                signal.market_id, signal.quantity, adjusted_qty,
                self.max_position_pct * 100,
            )

        # Kelly Criterion sizing
        kelly_qty = self._kelly_size(signal)
        if kelly_qty <= 0:
            inferred_edge = self._infer_signal_edge(signal)
            if (
                signal.confidence >= Config.KELLY_ONE_LOT_CONFIDENCE_FLOOR
                and inferred_edge >= Config.KELLY_ONE_LOT_EDGE_FLOOR
                and 0.0 < signal.target_price < 0.98
            ):
                kelly_qty = 1
                logger.info(
                    "Risk: Kelly fallback allowing 1-lot probe on %s (edge=%.3f, conf=%.2f)",
                    signal.market_id,
                    inferred_edge,
                    signal.confidence,
                )

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
        self._pending_updated_at[pos_key] = time.time()
        
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
        quarter_kelly = full_kelly * self.kelly_fraction

        # Convert to contract count
        kelly_value = self.current_equity * quarter_kelly
        contracts = int(kelly_value / buy_price)

        # Round up if the edge is substantial (>10%) to ensure we at least buy 1 contract
        if contracts <= 0 and (p - buy_price) >= 0.10:
            contracts = 1
            
        return max(0, contracts)

    def _infer_signal_edge(self, signal: Signal) -> float:
        """Estimate edge from signal metadata when explicit model outputs are available."""
        if signal.action != Side.BUY:
            return 0.0

        try:
            import re

            fair_match = re.search(r'fair=([\d.]+)%', signal.reason)
            if fair_match:
                fair_yes = float(fair_match.group(1)) / 100.0
                contract_fair = fair_yes
                if signal.contract_side == ContractSide.NO:
                    contract_fair = 1.0 - fair_yes
                return max(0.0, contract_fair - signal.target_price)
        except Exception:
            pass

        return max(0.0, signal.confidence - signal.target_price)

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
        notional = fill.fill_price * fill.fill_quantity

        if fill.order.side == Side.BUY:
            self._gross_buy_notional += notional
        else:
            self._gross_sell_notional += notional

        # Clear pending count for this market
        if pos_key in self._pending_signals:
            self._pending_signals[pos_key] = max(0, self._pending_signals[pos_key] - fill.fill_quantity)
            if self._pending_signals[pos_key] <= 0:
                del self._pending_signals[pos_key]
                self._pending_updated_at.pop(pos_key, None)
            else:
                self._pending_updated_at[pos_key] = time.time()

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
        self._write_journal_event(
            "fill",
            {
                "market_id": fill.order.market_id,
                "platform": fill.order.platform.value,
                "side": fill.order.side.value,
                "contract_side": fill.order.contract_side.value,
                "fill_quantity": fill.fill_quantity,
                "fill_price": fill.fill_price,
                "notional": notional,
                "fees": fill.fees,
                "open_notional_cost_basis": self._position_cost_basis(),
            },
        )

    def record_settlement(self, market_id: str, payout_per_contract: float):
        """Record a market settlement (contract resolved)."""
        settlement_events: list[dict[str, Any]] = []

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
            settlement_events.append(
                {
                    "market_id": market_id,
                    "contract_side": ContractSide.YES.value,
                    "quantity": pos.quantity,
                    "payout_per_contract": payout_per_contract,
                    "payout": payout,
                }
            )
            
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
            settlement_events.append(
                {
                    "market_id": market_id,
                    "contract_side": ContractSide.NO.value,
                    "quantity": pos.quantity,
                    "payout_per_contract": no_payout_per_contract,
                    "payout": payout,
                }
            )

        self.save_state()
        for event in settlement_events:
            self._write_journal_event("settlement", event)


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
                    self._pending_updated_at.pop(key, None)
                else:
                    self._pending_updated_at[key] = time.time()
            self._write_journal_event(
                "order_failure",
                {
                    "market_id": market_id,
                    "quantity": quantity,
                    "contract_side": contract_side.value,
                },
            )
            return

        # Fallback for legacy call sites: release all pending entries for this market.
        keys = [k for k in self._pending_signals.keys() if k[0] == market_id]
        for key in keys:
            self._pending_signals[key] = max(0, self._pending_signals[key] - quantity)
            if self._pending_signals[key] <= 0:
                del self._pending_signals[key]
                self._pending_updated_at.pop(key, None)
            else:
                self._pending_updated_at[key] = time.time()

        self._write_journal_event(
            "order_failure",
            {
                "market_id": market_id,
                "quantity": quantity,
                "contract_side": "unknown",
            },
        )

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
        self._pending_signals.clear()
        self._pending_updated_at.clear()
        self._pending_cleanup_last = time.time()
        logger.info("Circuit breaker manually reset")
        self._write_journal_event(
            "circuit_breaker_reset",
            {
                "pending_cleared": True,
            },
        )

    # ── Stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        total_fees = sum(f.fees for f in self._fills)
        total_trades = len(self._fills)
        open_notional_cost_basis = self._position_cost_basis()
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
            "gross_buy_notional": self._gross_buy_notional,
            "gross_sell_notional": self._gross_sell_notional,
            "open_notional_cost_basis": open_notional_cost_basis,
            "settlements_collected": self._settlements_collected,
            "risk_profile": self.risk_profile,
            "min_signal_confidence": self.min_signal_confidence,
            "max_position_pct": self.max_position_pct,
            "max_drawdown_limit_pct": self.max_drawdown_pct * 100,
            "kelly_fraction": self.kelly_fraction,
            "pending_signals": sum(self._pending_signals.values()),
            "session_uptime_seconds": int(max(0.0, time.time() - self._session_started_at)),
            "state_file": str(self._state_path),
            "journal_file": str(self._journal_path) if self._journal_path else None,
            "summary_file": str(self._summary_path) if self._summary_path else None,
            "pnl": self.current_equity - self.initial_equity,
            "pnl_pct": ((self.current_equity / self.initial_equity) - 1) * 100
            if self.initial_equity > 0 else 0,
        }
