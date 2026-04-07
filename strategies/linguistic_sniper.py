"""
Linguistic Sniper Strategy (LLM-Powered)
Analyzes Kalshi "Mention Market" rules using OpenAI to detect
linguistic edge cases where the contract's resolution criteria
differ from what the "dumb money" assumes.

Core idea from research:
  "An LLM-powered script can analyze the contract's 'Rules and Important
   Information' PDF and detect these linguistic traps, betting against the
   dumb money that hasn't read the fine print."
"""

import asyncio
import json
import logging
import time
from typing import Optional

from openai import AsyncOpenAI

from config import Config
from models import Signal, Side, ContractSide, Platform

logger = logging.getLogger(__name__)

# System prompt that instructs the LLM to be a contract analyst
SYSTEM_PROMPT = """You are a prediction market contract analyst specializing in
"Mention Markets" on Kalshi. Your job is to find linguistic edge cases where
the contract resolution rules differ from what casual traders would assume.

When given contract rules and a question, you must:
1. Identify the EXACT resolution criteria (word forms, plurals, tense, etc.)
2. Consider edge cases: "foster" vs "fosters", "inflation" vs "inflationary"
3. Determine if the market is likely mispriced based on linguistic ambiguity
4. Rate your confidence (0.0 to 1.0) that the market is exploitable

Respond ONLY with valid JSON in this format:
{
    "analysis": "Your detailed analysis of the linguistic edge case",
    "is_mispriced": true/false,
    "direction": "YES" or "NO",
    "confidence": 0.0 to 1.0,
    "key_trap": "The specific linguistic trap identified",
    "reasoning": "Why the mass of traders would be wrong"
}"""


class LinguisticSniperStrategy:
    """
    Uses OpenAI GPT-4o to analyze mention market contracts
    and detect exploitable linguistic ambiguities.
    """

    def __init__(self):
        self._client: Optional[AsyncOpenAI] = None
        self._active = False
        self._analysis_cache: dict[str, dict] = {}

    async def start(self):
        if not Config.OPENAI_API_KEY:
            logger.warning("Linguistic sniper: OPENAI_API_KEY not set, disabled")
            return

        self._client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)
        self._active = True
        logger.info("Linguistic sniper: started (model=%s)", Config.OPENAI_MODEL)

    async def stop(self):
        self._active = False

    async def analyze_contract(
        self,
        market_id: str,
        market_title: str,
        rules_text: str,
        current_yes_price: float,
        platform: Platform = Platform.KALSHI,
    ) -> Optional[Signal]:
        """
        Analyze a mention market contract for linguistic edge cases.

        Args:
            market_id: The market ticker/ID
            market_title: Human-readable title (e.g. "Will Melania say 'foster'?")
            rules_text: The full rules/resolution criteria text from the contract
            current_yes_price: Current YES price (0.0-1.0)
            platform: Which platform the contract is on
        """
        if not self._active or not self._client:
            return None

        # Check cache
        cache_key = f"{market_id}:{hash(rules_text)}"
        if cache_key in self._analysis_cache:
            cached = self._analysis_cache[cache_key]
            age = time.time() - cached.get("timestamp", 0)
            if age < 300:  # 5 minute cache
                return cached.get("signal")

        try:
            user_prompt = f"""Analyze this prediction market contract for linguistic edge cases:

**Market Title:** {market_title}

**Current YES Price:** ${current_yes_price:.2f} (implies {current_yes_price*100:.1f}% probability)

**Resolution Rules:**
{rules_text}

Based on the EXACT wording of the rules, is this market likely mispriced?
Consider: word forms, plurals, tense variants, compound words, abbreviations,
and whether the source transcript will match the resolution criteria literally."""

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

            result_text = response.choices[0].message.content
            result = json.loads(result_text)

            logger.info(
                "LLM analysis of %s: mispriced=%s, direction=%s, conf=%.2f — %s",
                market_id, result["is_mispriced"], result["direction"],
                result["confidence"], result.get("key_trap", "N/A"),
            )

            signal = None
            if result["is_mispriced"] and result["confidence"] >= 0.6:
                contract_side = ContractSide.YES if result["direction"] == "YES" else ContractSide.NO

                signal = Signal(
                    strategy="linguistic_sniper",
                    action=Side.BUY,
                    contract_side=contract_side,
                    platform=platform,
                    market_id=market_id,
                    target_price=current_yes_price,
                    quantity=self._size_for_confidence(result["confidence"]),
                    confidence=result["confidence"],
                    reason=f"LLM trap: {result.get('key_trap', 'Unknown')} — {result.get('reasoning', '')}",
                )

                logger.info(
                    "LINGUISTIC SNIPER SIGNAL: BUY %s @ %.2f (conf=%.2f)",
                    market_id, current_yes_price, result["confidence"],
                )

            # Cache result
            self._analysis_cache[cache_key] = {
                "result": result,
                "signal": signal,
                "timestamp": time.time(),
            }
            
            # Prevent cache leak
            if len(self._analysis_cache) > 2000:
                self._analysis_cache.clear()

            return signal

        except json.JSONDecodeError as e:
            logger.error("LLM returned invalid JSON: %s", e)
            return None
        except Exception as e:
            logger.error("LLM analysis failed: %s", e)
            return None

    async def scan_mention_markets(
        self,
        markets: list[dict],
        platform: Platform = Platform.KALSHI,
    ) -> list[Signal]:
        """
        Batch-analyze a list of mention markets.
        Each market dict should have: id, title, rules, yes_price
        """
        signals = []
        for market in markets:
            signal = await self.analyze_contract(
                market_id=market["id"],
                market_title=market["title"],
                rules_text=market.get("rules", ""),
                current_yes_price=market.get("yes_price", 0.5),
                platform=platform,
            )
            if signal:
                signals.append(signal)

            # Rate limit: don't spam the OpenAI API
            await asyncio.sleep(1.0)

        return signals

    def _size_for_confidence(self, confidence: float) -> int:
        """Higher LLM confidence → more aggressive sizing."""
        if confidence >= 0.9:
            return 75
        elif confidence >= 0.8:
            return 50
        elif confidence >= 0.7:
            return 30
        else:
            return 15

    def get_cache_stats(self) -> dict:
        return {
            "cached_analyses": len(self._analysis_cache),
            "signals_generated": sum(
                1 for v in self._analysis_cache.values() if v.get("signal") is not None
            ),
        }
