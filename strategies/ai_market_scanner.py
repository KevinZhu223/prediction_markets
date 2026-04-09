"""
AI Market Scanner Strategy (Kalshi-only)
Replaces the cross-platform arbitrage strategy with an LLM-powered
scanner that evaluates Kalshi markets for mispricing using GPT-4o.

Instead of comparing prices across platforms, this strategy:
1. Fetches active Kalshi markets
2. Uses GPT-4o to estimate fair probability for each event
3. Compares the AI estimate to the market price
4. When the edge exceeds a threshold, generates a trading signal

This exploits the behavioral inefficiency where retail traders
over/underreact to news, creating temporary mispricings.
"""

import asyncio
import json
import logging
import time
from collections import deque
from typing import Optional

from openai import AsyncOpenAI

from config import Config
from models import Signal, Side, Platform, ContractSide
from exchanges.kalshi_wrapper import KalshiExchange
from engine.runtime_lock import acquire_single_instance_lock

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a quantitative analyst evaluating prediction market events.
You will be given a market title, its current probability, and any available context.

Your job is to estimate the TRUE probability of the event occurring based on:
- Current news and geopolitical context
- Historical base rates for similar events
- Any logical reasoning about the event

Respond ONLY with valid JSON:
{
    "fair_probability": 0.0 to 1.0,
    "confidence": 0.0 to 1.0,
    "reasoning": "Brief explanation of your estimate",
    "edge_direction": "OVER" or "UNDER" or "FAIR",
    "key_factors": ["factor1", "factor2"]
}

OVER means the market price is too high (bet NO / sell YES).
UNDER means the market price is too low (bet YES / buy YES).
FAIR means the market is correctly priced."""


class AIMarketScanner:
    """
    Uses OpenAI GPT-4o to scan Kalshi markets and detect mispricings.
    Replaces the cross-platform arbitrage that required Polymarket.
    """

    def __init__(self, kalshi: KalshiExchange, pessimistic_mode: bool = True):
        self.kalshi = kalshi
        self.pessimistic_mode = pessimistic_mode
        self.mode = Config.SCANNER_MODE
        self._client: Optional[AsyncOpenAI] = None
        self._active = False
        self.min_edge = Config.SCANNER_MIN_EDGE
        self.scan_interval = Config.SCANNER_INTERVAL_SECONDS
        self.max_markets_per_scan = max(1, Config.SCANNER_MAX_MARKETS_PER_SCAN)
        self._min_volume = max(0.0, Config.SCANNER_MIN_VOLUME)

        # Cache to avoid re-analyzing the same market
        self._analysis_cache: dict[str, dict] = {}
        self._cache_ttl = max(60, Config.SCANNER_CACHE_TTL_SECONDS)
        self._market_cooldown_seconds = max(
            self._cache_ttl,
            Config.SCANNER_MARKET_COOLDOWN_SECONDS,
        )

        # OpenAI burn controls
        self._max_calls_per_hour = max(1, Config.SCANNER_MAX_CALLS_PER_HOUR)
        self._daily_token_budget = max(1000, Config.OPENAI_DAILY_TOKEN_BUDGET)
        self._hourly_call_timestamps: deque[float] = deque()
        self._daily_tokens_used = 0
        self._daily_usage_date = time.strftime("%Y-%m-%d")
        self._market_last_eval: dict[str, float] = {}
        self._scanner_lock_name = "ai_scanner_llm"
        self._openai_cooldown_until = 0.0
        self._last_openai_cooldown_log = 0.0
        self._openai_min_confidence = 0.65
        self._openai_min_call_spacing_seconds = max(
            0.25,
            min(2.0, self.scan_interval / max(1, self.max_markets_per_scan)),
        )

        # Track all signals generated
        self._signals_generated: list[Signal] = []
        self._markets_scanned = 0
        self._opportunities_found = 0

        # Track markets we've already signaled to avoid re-buying every cycle
        self._signaled_markets: set[str] = set()

    async def start(self):
        if not Config.SCANNER_ENABLED:
            logger.info("AI scanner: disabled via SCANNER_ENABLED=false")
            return

        if self.mode != "llm":
            logger.info(
                "AI scanner: disabled (SCANNER_MODE=%s). Set SCANNER_MODE=llm to enable OpenAI calls.",
                self.mode,
            )
            return

        if not Config.OPENAI_API_KEY:
            logger.warning("AI scanner: OPENAI_API_KEY not set, disabled")
            return

        try:
            acquire_single_instance_lock(
                self._scanner_lock_name,
                takeover_existing=False,
            )
        except RuntimeError as e:
            logger.warning("AI scanner: %s", e)
            return

        self._client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)
        self._active = True
        logger.info(
            "AI scanner: started (model=%s, min_edge=%.2f, interval=%.0fs, max_scan=%d, max_calls_hr=%d, token_budget_day=%d)",
            Config.OPENAI_MODEL,
            self.min_edge,
            self.scan_interval,
            self.max_markets_per_scan,
            self._max_calls_per_hour,
            self._daily_token_budget,
        )

    async def stop(self):
        self._active = False

    async def run_scan_loop(self, signal_callback=None):
        """Continuously scan Kalshi markets for mispricings."""
        while self._active:
            try:
                signals = await self.scan_markets()
                if signals and signal_callback:
                    for s in signals:
                        await signal_callback(s)
            except Exception as e:
                logger.error("AI scanner loop error: %s", e)

            await asyncio.sleep(self.scan_interval)

    async def scan_markets(self, categories: list[str] = None) -> list[Signal]:
        """
        Fetch active Kalshi markets and evaluate each for mispricing.
        """
        if not self._active or not self._client:
            return []

        signals = []

        now = time.time()
        if now < self._openai_cooldown_until:
            if now - self._last_openai_cooldown_log >= 30:
                logger.info(
                    "AI scanner: OpenAI cooldown active for %.0fs",
                    self._openai_cooldown_until - now,
                )
                self._last_openai_cooldown_log = now
            return []

        try:
            markets = await self.kalshi.fetch_markets()
            self._markets_scanned = len(markets)

            # Filter to interesting markets
            interesting = [
                m for m in markets
                if m.get("status") == "active"
                and self._is_interesting_market(m)
            ]

            # Prioritize higher-liquidity markets first.
            interesting.sort(
                key=lambda m: float(m.get("volume_fp", 0) or 0),
                reverse=True,
            )

            candidates = []
            now = time.time()
            for market in interesting:
                ticker = market.get("ticker", "")
                if not ticker:
                    continue
                if ticker in self._signaled_markets:
                    continue
                last_eval = self._market_last_eval.get(ticker)
                if last_eval and (now - last_eval) < self._market_cooldown_seconds:
                    continue
                candidates.append(market)

            logger.info(
                "AI scanner: candidates=%d/%d (scan_cap=%d, calls_hr=%d/%d, tokens_today=%d/%d)",
                len(candidates),
                len(markets),
                self.max_markets_per_scan,
                self._calls_last_hour(),
                self._max_calls_per_hour,
                self._daily_tokens_used,
                self._daily_token_budget,
            )

            for market in candidates[: self.max_markets_per_scan]:
                if not self._can_make_openai_call():
                    pause_seconds = self._seconds_until_next_openai_slot()
                    if self._daily_tokens_used >= self._daily_token_budget:
                        pause_seconds = max(pause_seconds, max(self.scan_interval, 60.0))
                    pause_seconds = max(5.0, pause_seconds)
                    self._set_openai_cooldown(pause_seconds)
                    logger.warning(
                        "AI scanner: OpenAI budget reached, cooling down %.0fs",
                        pause_seconds,
                    )
                    break

                ticker = market.get("ticker", "")

                self._market_last_eval[ticker] = time.time()

                signal = await self._evaluate_market(market)
                if signal:
                    signals.append(signal)
                    self._signals_generated.append(signal)
                    self._opportunities_found += 1
                    self._signaled_markets.add(ticker)

                # Rate limit OpenAI calls
                await asyncio.sleep(self._openai_min_call_spacing_seconds)

                if time.time() < self._openai_cooldown_until:
                    break

        except Exception as e:
            logger.error("Market scan failed: %s", e)

        return signals

    async def _evaluate_market(self, market: dict) -> Optional[Signal]:
        """Evaluate a single Kalshi market against AI fair value."""
        ticker = market.get("ticker", "")
        title = market.get("title", "")

        # Elections API uses dollar-denominated fields
        yes_bid_d = market.get("yes_bid_dollars")
        yes_ask_d = market.get("yes_ask_dollars")
        yes_price = float(yes_bid_d) if yes_bid_d and float(yes_bid_d) > 0 else None
        yes_ask = float(yes_ask_d) if yes_ask_d and float(yes_ask_d) > 0 else None

        # Use mid price if both available, otherwise best available
        if yes_price and yes_ask:
            market_prob = (yes_price + yes_ask) / 2
        elif yes_ask:
            market_prob = yes_ask
        elif yes_price:
            market_prob = yes_price
        else:
            return None

        # Check cache
        cache_key = ticker
        if cache_key in self._analysis_cache:
            cached = self._analysis_cache[cache_key]
            if time.time() - cached["timestamp"] < self._cache_ttl:
                return cached.get("signal")

        try:
            user_prompt = f"""Evaluate this prediction market:

Market: {title}
Ticker: {ticker}
Current Market Probability: {market_prob:.1%}
Current Date: {time.strftime('%Y-%m-%d')}

Return compact JSON only. Keep reasoning <= 25 words."""

            response = await self._client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
                max_tokens=Config.OPENAI_MAX_OUTPUT_TOKENS,
                response_format={"type": "json_object"},
            )

            usage = getattr(response, "usage", None)
            if usage is not None:
                total_tokens = int(
                    getattr(usage, "total_tokens", 0)
                    or (
                        int(getattr(usage, "prompt_tokens", 0) or 0)
                        + int(getattr(usage, "completion_tokens", 0) or 0)
                    )
                )
            else:
                total_tokens = max(1, (len(SYSTEM_PROMPT) + len(user_prompt)) // 4)
            self._register_openai_usage(total_tokens)

            result = json.loads(response.choices[0].message.content)
            fair_prob = float(result.get("fair_probability", market_prob))
            fair_prob = min(0.99, max(0.01, fair_prob))
            confidence = float(result.get("confidence", 0.5))
            confidence = min(1.0, max(0.0, confidence))
            direction = result.get("edge_direction", "FAIR")
            reasoning = result.get("reasoning", "")

            edge = abs(fair_prob - market_prob)
            required_confidence = 0.60 if edge >= 0.12 else self._openai_min_confidence

            logger.info(
                "AI scan %s: market=%.1f%%, fair=%.1f%%, edge=%.1f%%, dir=%s — %s",
                ticker, market_prob * 100, fair_prob * 100,
                edge * 100, direction, reasoning[:80],
            )

            signal = None
            if edge >= self.min_edge and confidence >= required_confidence and direction != "FAIR":
                if direction == "UNDER":
                    # Market is underpriced → buy YES
                    signal = Signal(
                        strategy="ai_scanner",
                        action=Side.BUY,
                        contract_side=ContractSide.YES,
                        platform=Platform.KALSHI,
                        market_id=ticker,
                        target_price=yes_ask if yes_ask else market_prob,
                        quantity=self._size_for_edge(edge, confidence),
                        confidence=confidence,
                        reason=f"AI: fair={fair_prob:.1%} vs market={market_prob:.1%} ({reasoning[:100]})",
                    )
                elif direction == "OVER":
                    # Market is overpriced → buy NO
                    signal = Signal(
                        strategy="ai_scanner",
                        action=Side.BUY,
                        contract_side=ContractSide.NO,
                        platform=Platform.KALSHI,
                        market_id=ticker,
                        target_price=(1.0 - yes_price) if yes_price else (1.0 - market_prob),
                        quantity=self._size_for_edge(edge, confidence),
                        confidence=confidence,
                        reason=f"AI: fair={fair_prob:.1%} vs market={market_prob:.1%} ({reasoning[:100]})",
                    )

                if signal:
                    # SAFETY CAP: Never buy AI contracts at > $0.85 (implied 85% probability).
                    # These carry asymmetrical risk-reward where 1 loss wipes out 10+ wins.
                    if signal.target_price > 0.85:
                        logger.info("AI SCANNER: Skipping signal %s @ %.4f (Price %d¢ exceeds 85¢ safety cap)", 
                                    signal.market_id, signal.target_price, int(signal.target_price*100))
                        return None

                    logger.info(
                        "AI SCANNER SIGNAL: %s %s @ %.4f (edge=%.1f%%, conf=%.2f)",
                        signal.action.value, signal.market_id,
                        signal.target_price, edge * 100, confidence,
                    )

            # Cache
            self._analysis_cache[cache_key] = {
                "result": result,
                "signal": signal,
                "timestamp": time.time(),
            }

            return signal

        except json.JSONDecodeError as e:
            logger.error("AI scanner: invalid JSON from LLM: %s", e)
            return None
        except Exception as e:
            status_code = getattr(e, "status_code", None)
            msg = str(e).lower()
            if status_code == 429 or "rate limit" in msg or "429" in msg:
                cooldown = max(15.0, self.scan_interval * 2)
                self._set_openai_cooldown(cooldown)
                logger.warning(
                    "AI scanner: OpenAI rate limited for %s, cooling down %.0fs",
                    ticker,
                    cooldown,
                )
                return None
            logger.error("AI scanner: evaluation failed for %s: %s", ticker, e)
            return None

    def _is_interesting_market(self, market: dict) -> bool:
        """Filter out markets that aren't worth scanning."""
        ticker = market.get("ticker", "")
        title = market.get("title", "").lower()

        # Skip multi-game sports parlays (zero liquidity junk)
        if ticker.startswith("KXMVE"):
            return False

        # Skip markets with very low liquidity
        volume = float(market.get("volume_fp", 0) or 0)
        if volume < self._min_volume:
            return False

        # Skip already-decided markets  (prices in dollars: 0.00 - 1.00)
        yes_bid = float(market.get("yes_bid_dollars", 0) or 0)
        if yes_bid <= 0.02 or yes_bid >= 0.98:
            return False
        if self.pessimistic_mode:
            # PESSIMISTIC PIVOT: Skip pure quantitative markets (Sports, Finance, Weather)
            # where we have zero edge against institutional HFT models.
            skip_words = ["points", "rebounds", "assists", "temperature", "precip", "cpi", "gdp", "fed rate", "inflation", "unemployment"]
            if any(w in title for w in skip_words):
                return False
                
            # Filter out major sports tickers
            if ticker.startswith("KXNBA") or ticker.startswith("KXNFL") or ticker.startswith("KXMLB") or ticker.startswith("KXNHL"):
                return False

        return True

    def _refresh_usage_windows(self):
        """Roll hourly and daily OpenAI usage windows forward."""
        now = time.time()
        while self._hourly_call_timestamps and now - self._hourly_call_timestamps[0] > 3600:
            self._hourly_call_timestamps.popleft()

        today = time.strftime("%Y-%m-%d")
        if today != self._daily_usage_date:
            self._daily_usage_date = today
            self._daily_tokens_used = 0

    def _calls_last_hour(self) -> int:
        self._refresh_usage_windows()
        return len(self._hourly_call_timestamps)

    def _can_make_openai_call(self) -> bool:
        self._refresh_usage_windows()
        return (
            len(self._hourly_call_timestamps) < self._max_calls_per_hour
            and self._daily_tokens_used < self._daily_token_budget
        )

    def _register_openai_usage(self, total_tokens: int):
        self._refresh_usage_windows()
        self._hourly_call_timestamps.append(time.time())
        self._daily_tokens_used += max(0, total_tokens)

    def _seconds_until_next_openai_slot(self) -> float:
        self._refresh_usage_windows()
        if len(self._hourly_call_timestamps) < self._max_calls_per_hour:
            return 0.0
        oldest_call = self._hourly_call_timestamps[0]
        return max(0.0, 3600.0 - (time.time() - oldest_call))

    def _set_openai_cooldown(self, seconds: float):
        if seconds <= 0:
            return
        self._openai_cooldown_until = max(
            self._openai_cooldown_until,
            time.time() + seconds,
        )

    def _size_for_edge(self, edge: float, confidence: float) -> int:
        """Size based on edge and confidence."""
        base = 10
        if edge >= 0.20 and confidence >= 0.8:
            base = 50
        elif edge >= 0.15:
            base = 30
        elif edge >= 0.10:
            base = 20
        return base

    def get_stats(self) -> dict:
        return {
            "mode": self.mode,
            "markets_scanned": self._markets_scanned,
            "opportunities_found": self._opportunities_found,
            "signals_generated": len(self._signals_generated),
            "cache_size": len(self._analysis_cache),
            "openai_calls_last_hour": self._calls_last_hour(),
            "openai_tokens_today": self._daily_tokens_used,
            "openai_daily_token_budget": self._daily_token_budget,
        }
