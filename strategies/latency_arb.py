"""
Latency Arbitrage Strategy (Kalshi-only)
Exploits the delay between crypto spot price movements and
Kalshi order book updates for 15-min recurring markets.

KXBTC15M / KXETH15M markets are directional: "Will BTC be higher at
the end of the 15 minutes than at the start?"  The market has a
`floor_strike` field that is the opening reference price.  YES wins
if the 1-minute BRTI average at close >= floor_strike.

The OLD logic only reacted to local micro-jumps which caused the bot
to buy YES during a brief bounce even when spot was still well below
the strike.  The NEW logic checks current spot vs floor_strike.

TIME-DECAY FIX (v3): The bot now tracks the expiration time of each
15-minute interval.  Fair-value estimates are scaled DOWN when there
is a lot of time remaining (price can retrace), and scaled UP in the
final 1-3 minutes when the outcome is nearly locked in.  This prevents
the bot from overpaying for contracts early in the interval.
"""

import asyncio
import logging
import time
from typing import Optional

from config import Config
from models import (
    Signal, Side, Platform, ContractSide, SpotPriceUpdate, OrderBook,
)
from data.aggregator import DataAggregator

logger = logging.getLogger(__name__)


class LatencyArbStrategy:
    """
    Monitors Coinbase for rapid BTC/ETH price movements and generates
    signals for the corresponding 15-minute directional prediction
    markets on Kalshi.

    Key improvement: the bot now tracks the floor_strike (interval
    opening price) from Kalshi's market data and only generates signals
    in the correct direction relative to the strike.
    """

    def __init__(self, aggregator: DataAggregator):
        self.aggregator = aggregator
        self.jump_threshold = Config.SPOT_PRICE_JUMP_THRESHOLD
        self.min_edge = Config.MIN_EDGE_THRESHOLD
        self._min_entry_price = max(0.0, Config.LATENCY_MIN_ENTRY_PRICE)
        self._max_entry_price = min(1.0, Config.LATENCY_MAX_ENTRY_PRICE)
        self._active = False

        # Cooldown tracking to avoid re-entering the same move
        self._last_signal_time: dict[str, float] = {}
        self._cooldown_seconds = 30.0

        # Market Series mappings: maps spot symbol to Kalshi Series Tickers
        self.market_mappings: dict[str, dict] = {
            "BTCUSDT": {
                "series": "KXBTC15M", # Default to 15-min BTC
                "active_ticker": None,
                "floor_strike": None,  # Opening reference price
            },
            "ETHUSDT": {
                "series": "KXETH15M", # Default to 15-min ETH
                "active_ticker": None,
                "floor_strike": None,
            },
        }

        # Cache the latest spot price per symbol for strike comparison
        self._latest_spot: dict[str, float] = {}

    def configure_markets(self, mappings: dict[str, dict]):
        """Set the series tickers at runtime."""
        self.market_mappings.update(mappings)
        logger.info("Latency arb: configured %d symbol mappings", len(mappings))

    async def start(self):
        """Register with the aggregator and begin monitoring."""
        self._active = True
        self.aggregator.binance.on_update(self._on_spot_update)
        
        # Start the ticker discovery loop
        asyncio.create_task(self._discovery_loop())
        
        logger.info(
            "Latency arb: started (threshold=$%.2f, cooldown=%.0fs)",
            self.jump_threshold, self._cooldown_seconds,
        )

    async def stop(self):
        self._active = False

    async def _discovery_loop(self):
        """Periodically refresh the active tickers and floor_strike for each series."""
        while self._active:
            try:
                for symbol, data in self.market_mappings.items():
                    series = data.get("series")
                    if series:
                        new_ticker = await self.aggregator.kalshi.get_active_series_ticker(series)
                        if new_ticker and new_ticker != data.get("active_ticker"):
                            logger.info("Latency arb: updated %s active ticker to %s", symbol, new_ticker)
                            data["active_ticker"] = new_ticker

                        # Fetch the floor_strike and expiration_time for the active market
                        if new_ticker:
                            try:
                                market_detail = await self.aggregator.kalshi.get_market(new_ticker)
                                if market_detail:
                                    fs = market_detail.get("floor_strike")
                                    if fs is not None:
                                        data["floor_strike"] = float(fs)

                                    # Cache the expiration time so we can compute time-to-expiry
                                    exp_str = market_detail.get("expiration_time", "")
                                    if exp_str:
                                        try:
                                            from datetime import datetime, timezone
                                            exp_dt = datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
                                            data["expiration_ts"] = exp_dt.timestamp()
                                        except Exception:
                                            pass

                                    logger.info(
                                        "Latency arb: %s floor_strike=$%.2f, expires_in=%.1fmin",
                                        new_ticker,
                                        data.get("floor_strike", 0),
                                        max(0, (data.get("expiration_ts", 0) - time.time())) / 60.0,
                                    )
                            except Exception as e:
                                logger.debug("Latency arb: floor_strike fetch error for %s: %s", new_ticker, e)
            except Exception as e:
                logger.error("Ticker discovery error: %s", e)
            
            await asyncio.sleep(60) # check every minute

    async def _on_spot_update(
        self,
        update: SpotPriceUpdate,
        delta: Optional[float],
    ):
        """Called on every price update. Acts on jumps."""
        if not self._active or delta is None:
            return

        symbol = update.symbol
        now = time.time()

        # Always track the latest spot price
        self._latest_spot[symbol] = update.price

        # Check cooldown
        last = self._last_signal_time.get(symbol, 0)
        if now - last < self._cooldown_seconds:
            return

        data = self.market_mappings.get(symbol, {})
        active_ticker = data.get("active_ticker")
        floor_strike = data.get("floor_strike")
        if not active_ticker or floor_strike is None:
            return

        # Compute minutes remaining until this 15-minute interval expires
        expiration_ts = data.get("expiration_ts")
        if expiration_ts is None:
            return
        minutes_remaining = max(0.0, (expiration_ts - now) / 60.0)

        # Gate: skip if market already expired (stale ticker)
        if minutes_remaining <= 0:
            return

        # Gate: configurable max minutes before expiry (default 15 = entire interval)
        if minutes_remaining > Config.LATENCY_MAX_MINUTES_BEFORE_EXPIRY:
            return

        # Core fix: determine the TRUE direction relative to the interval strike.
        # The market resolves YES if spot >= floor_strike at expiry.
        spot = update.price
        distance_from_strike = spot - floor_strike
        pct_distance = abs(distance_from_strike) / floor_strike if floor_strike > 0 else 0

        # HARD GATE: Block all signals when >5 minutes remain UNLESS
        # the move is extraordinary (>0.5% from strike). Small moves with
        # 10+ minutes left are just noise and will revert.
        if minutes_remaining > 5.0 and pct_distance < 0.005:
            return

        # Also require a minimum absolute distance to avoid noise trades
        if pct_distance < 0.0015:  # 0.15% minimum (~$120 on BTC)
            return

        # Only generate signals when the jump agrees with the strike direction.
        if delta > 0 and distance_from_strike > 0:
            # Price jumped UP and we're above the strike → YES is likely
            signals = self._generate_up_signals(symbol, delta, active_ticker, spot, floor_strike, minutes_remaining)
        elif delta < 0 and distance_from_strike < 0:
            # Price dropped DOWN and we're below the strike → NO is likely
            signals = self._generate_down_signals(symbol, delta, active_ticker, spot, floor_strike, minutes_remaining)
        else:
            # Jump direction disagrees with strike position — skip
            return

        if signals:
            self._last_signal_time[symbol] = now
            return signals

    def _generate_up_signals(
        self, symbol: str, delta: float, ticker: str,
        spot: float, floor_strike: float, minutes_remaining: float,
    ) -> list[Signal]:
        """Spot > strike after an UP jump: Buy YES."""
        signals = []
        distance = spot - floor_strike  # positive when above strike
        confidence = min(0.95, 0.50 + (distance / floor_strike) * 200)

        # Apply time-decay to confidence: early-interval signals are unreliable
        confidence = self._apply_time_decay_confidence(confidence, minutes_remaining)

        ob = self.aggregator.get_order_book(ticker)
        if ob and ob.best_ask is not None:
            if not (self._min_entry_price <= ob.best_ask <= self._max_entry_price):
                logger.debug(
                    "Latency arb: skip %s YES ask %.4f outside [%.2f, %.2f]",
                    ticker,
                    ob.best_ask,
                    self._min_entry_price,
                    self._max_entry_price,
                )
                return signals

            # Estimate edge with time-decay: early signals get much lower fair values
            edge = self._estimate_edge(ob.best_ask, distance=distance, strike=floor_strike, minutes_remaining=minutes_remaining)
            if edge >= self.min_edge:
                signals.append(Signal(
                    strategy="latency_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=ob.best_ask,
                    quantity=self._size_for_edge(edge, minutes_remaining),
                    confidence=confidence,
                    reason=(
                        f"{symbol} spot=${spot:,.2f} > strike=${floor_strike:,.2f} "
                        f"(+${distance:,.2f}), jump=${delta:,.2f}, "
                        f"YES ask={ob.best_ask:.4f}, edge={edge:.4f}, "
                        f"mins_left={minutes_remaining:.1f}"
                    ),
                ))

        return signals

    def _generate_down_signals(
        self, symbol: str, delta: float, ticker: str,
        spot: float, floor_strike: float, minutes_remaining: float,
    ) -> list[Signal]:
        """Spot < strike after a DOWN jump: Buy NO."""
        signals = []
        distance = floor_strike - spot  # positive when below strike
        confidence = min(0.95, 0.50 + (distance / floor_strike) * 200)

        # Apply time-decay to confidence
        confidence = self._apply_time_decay_confidence(confidence, minutes_remaining)

        ob = self.aggregator.get_order_book(ticker)
        if ob and ob.best_bid is not None:
            # Buying NO at (1 - YES_bid)
            no_ask = 1.0 - ob.best_bid
            if not (self._min_entry_price <= no_ask <= self._max_entry_price):
                logger.debug(
                    "Latency arb: skip %s NO ask %.4f outside [%.2f, %.2f]",
                    ticker,
                    no_ask,
                    self._min_entry_price,
                    self._max_entry_price,
                )
                return signals

            edge = self._estimate_edge(no_ask, distance=distance, strike=floor_strike, minutes_remaining=minutes_remaining)
            
            if edge >= self.min_edge:
                signals.append(Signal(
                    strategy="latency_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.NO,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=no_ask,
                    quantity=self._size_for_edge(edge, minutes_remaining),
                    confidence=confidence,
                    reason=(
                        f"{symbol} spot=${spot:,.2f} < strike=${floor_strike:,.2f} "
                        f"(-${distance:,.2f}), jump=${delta:,.2f}, "
                        f"NO ask={no_ask:.4f}, edge={edge:.4f}, "
                        f"mins_left={minutes_remaining:.1f}"
                    ),
                ))

        return signals

    @staticmethod
    def _apply_time_decay_confidence(raw_confidence: float, minutes_remaining: float) -> float:
        """
        Scale down confidence for early-interval signals.

        With 14 minutes left, BTC can retrace any move.  With 1 minute left
        the observation is nearly final.  v4: Much more aggressive discounting.
        """
        if minutes_remaining <= 1.0:
            decay = 1.0       # full confidence — essentially settled
        elif minutes_remaining <= 2.0:
            decay = 0.90      # very high — locked in
        elif minutes_remaining <= 3.0:
            decay = 0.75      # high confidence
        elif minutes_remaining <= 5.0:
            decay = 0.55      # moderate — BTC moves fast
        elif minutes_remaining <= 10.0:
            decay = 0.30      # low — lots of time for reversion
        else:
            decay = 0.20      # very low — basically noise
        return raw_confidence * decay

    def _estimate_edge(
        self, current_ask: float, *, distance: float, strike: float,
        minutes_remaining: float,
    ) -> float:
        """
        Estimate the edge vs fair value based on distance from the floor_strike
        AND the time remaining in the 15-minute interval.

        The key insight: a 0.15% distance from strike with 14 minutes left is
        NOT worth anything — it's pure noise. Even with 5 minutes left, small
        moves are unreliable because Kalshi settles on a 1-minute BRTI average
        which can differ significantly from instantaneous spot.

        v4 TIGHTENING: Raised minimum distance thresholds significantly.
        A $80 BTC move (0.1%) used to trigger trades; now we need $200+ (0.25%).

        distance: absolute $ distance between spot and strike (always positive).
        strike:   the floor_strike value, used for normalisation.
        minutes_remaining: minutes until the 15-minute interval expires.
        """
        if strike <= 0:
            return 0.0

        pct_distance = distance / strike  # e.g. $200 / $80000 = 0.25%

        # v4: Much stricter distance tiers. Old thresholds were letting through
        # noise trades (0.1% moves = ~$80 BTC) that reverted before settlement.
        if pct_distance >= 0.007:      # >=0.7% away from strike (~$560 BTC)
            base_fair = 0.92
        elif pct_distance >= 0.005:    # >=0.5% (~$400)
            base_fair = 0.85
        elif pct_distance >= 0.003:    # >=0.3% (~$240)
            base_fair = 0.72
        elif pct_distance >= 0.0025:   # >=0.25% (~$200)
            base_fair = 0.62
        elif pct_distance >= 0.002:    # >=0.2% (~$160)
            base_fair = 0.56
        else:
            # Below 0.2% distance: NOT tradeable. Any fair value <= 0.50
            # will produce zero or negative edge vs any realistic ask price.
            base_fair = 0.50

        # TIME DECAY v4: Much more aggressive discounting for early intervals.
        # The old multipliers were too generous — 0.40 at 15 min still generated
        # positive edge on cheap contracts, leading to high-volume losing trades.
        if minutes_remaining <= 1.0:
            # Final minute — price is locked in, full fair value
            time_multiplier = 1.0
        elif minutes_remaining <= 2.0:
            # Last 2 minutes — very high confidence
            time_multiplier = 0.92
        elif minutes_remaining <= 3.0:
            # Last 3 minutes — high confidence
            time_multiplier = 0.80
        elif minutes_remaining <= 5.0:
            # 3-5 minutes — moderate, but BTC can still move 0.1% easily
            time_multiplier = 0.60
        elif minutes_remaining <= 10.0:
            # 5-10 minutes — very risky, aggressive discount
            time_multiplier = 0.35
        else:
            # >10 minutes — nearly worthless signal, huge reversion risk
            time_multiplier = 0.20

        # The time-adjusted fair value
        adjusted_fair = 0.50 + (base_fair - 0.50) * time_multiplier

        edge = max(adjusted_fair - current_ask, 0.0)

        logger.debug(
            "Latency edge calc: dist_pct=%.4f%%, base_fair=%.2f, "
            "time_mult=%.2f (%.1fmin), adj_fair=%.2f, ask=%.2f, edge=%.4f",
            pct_distance * 100, base_fair, time_multiplier,
            minutes_remaining, adjusted_fair, current_ask, edge,
        )

        return edge

    def _size_for_edge(self, edge: float, minutes_remaining: float) -> int:
        """
        Position sizing that scales with both edge magnitude AND time remaining.
        We size up aggressively in the final minutes when our signal is reliable,
        and stay small early when there's high reversion risk.
        """
        # Base size from edge
        if edge >= 0.15:
            base_size = 100
        elif edge >= 0.10:
            base_size = 50
        else:
            base_size = 20

        # Time-based size multiplier: bigger in the final minutes
        if minutes_remaining <= 2.0:
            return base_size            # full size — high conviction window
        elif minutes_remaining <= 5.0:
            return max(1, base_size // 2)   # half size — moderate conviction
        else:
            return max(1, base_size // 5)   # 1/5 size — probing only

