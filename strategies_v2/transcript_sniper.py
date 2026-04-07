"""
Real-Time Transcript Sniper Strategy

Monitors live broadcasts (TV shows, political speeches, podcasts)
via audio transcription and trades Kalshi "mention" markets the
instant a keyword is detected.

How it works:
  1. Kalshi has markets like "Will SNL mention Elon Musk?" or
     "Will Trump say 'tariff' this week?"
  2. We use a real-time audio transcription service to process
     the live broadcast audio stream.
  3. The moment a target keyword appears in the transcript,
     we sweep the YES contract.

Supported transcription backends:
  - OpenAI Whisper (via API) — batch mode, ~5s latency
  - Deepgram (streaming) — real-time, <1s latency (requires API key)
  - Local Whisper (whisper.cpp) — offline, ~3s latency

Target Kalshi series:
  - KXSNLMENTION     : SNL mention markets
  - KXMTPMENTION     : Meet the Press mention markets  
  - KXTRUMPSAY       : Weekly Trump speech keyword markets
  - KXMRBEASTMENTION : MrBeast video mention markets

Architecture:
  TranscriptSniper
    ├── AudioStreamCapture (grabs audio from YouTube/Twitch)
    ├── TranscriptionEngine (converts audio → text)
    ├── KeywordMatcher (detects target words in transcript)
    └── SignalEmitter (generates trading signals)
"""

import asyncio
import logging
import time
import re
from datetime import datetime, timezone
from typing import Optional, Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TranscriptChunk:
    """A chunk of transcribed audio."""
    text: str
    timestamp: float
    confidence: float
    source: str      # "whisper", "deepgram", etc.
    is_final: bool   # True if this is a finalized transcript segment


from models import Signal, Side, ContractSide, Platform


@dataclass
class MentionTarget:
    """A keyword to monitor for a specific Kalshi market."""
    kalshi_series: str
    keywords: list[str]      # Words/phrases to detect
    stream_url: str          # YouTube/Twitch URL to monitor
    show_name: str           # Human-readable show name
    market_ticker: Optional[str] = None
    air_time_utc: Optional[datetime] = None
    is_active: bool = False
    detected: bool = False


# ── Pre-configured Mention Targets ──────────────────────────────────────────

DEFAULT_TARGETS = [
    MentionTarget(
        kalshi_series="KXSNLMENTION",
        keywords=[],  # Populated dynamically from Kalshi market titles
        stream_url="",  # Set to the live SNL stream URL when known
        show_name="Saturday Night Live",
    ),
    MentionTarget(
        kalshi_series="KXMTPMENTION",
        keywords=[],
        stream_url="",
        show_name="Meet the Press",
    ),
    MentionTarget(
        kalshi_series="KXTRUMPSAY",
        keywords=[],
        stream_url="",
        show_name="Trump Speech/Rally",
    ),
]


class WhisperTranscriber:
    """
    Transcription engine using OpenAI Whisper API.
    
    For production use, consider switching to Deepgram's streaming API
    for lower latency (<1s vs ~5s).
    """

    def __init__(self, api_key: str, model: str = "whisper-1"):
        self.api_key = api_key
        self.model = model
        self._client = None

    async def start(self):
        """Initialize the OpenAI client."""
        try:
            from openai import AsyncOpenAI
            self._client = AsyncOpenAI(api_key=self.api_key)
            logger.info("Whisper transcriber: initialized (model=%s)", self.model)
        except ImportError:
            logger.error("openai package not installed")

    async def transcribe_audio(self, audio_data: bytes, language: str = "en") -> Optional[TranscriptChunk]:
        """
        Transcribe an audio chunk using Whisper.
        
        Args:
            audio_data: Raw audio bytes (WAV, MP3, etc.)
            language: Language code
            
        Returns:
            TranscriptChunk with the transcribed text
        """
        if not self._client:
            return None

        try:
            # Write audio to a temp buffer
            import io
            audio_file = io.BytesIO(audio_data)
            audio_file.name = "audio.wav"

            response = await self._client.audio.transcriptions.create(
                model=self.model,
                file=audio_file,
                language=language,
                response_format="verbose_json",
            )

            return TranscriptChunk(
                text=response.text,
                timestamp=time.time(),
                confidence=0.85,  # Whisper doesn't return confidence per-segment
                source="whisper",
                is_final=True,
            )

        except Exception as e:
            logger.error("Whisper transcription failed: %s", e)
            return None

    async def close(self):
        pass


class KeywordMatcher:
    """
    Matches keywords in transcript text with fuzzy matching support.
    """

    def __init__(self):
        self._compiled_patterns: dict[str, re.Pattern] = {}

    def add_keywords(self, market_id: str, keywords: list[str]):
        """Register keywords for a specific market."""
        # Build a regex that matches any of the keywords
        escaped = [re.escape(kw) for kw in keywords]
        pattern = re.compile(
            r'\b(' + '|'.join(escaped) + r')\b',
            re.IGNORECASE,
        )
        self._compiled_patterns[market_id] = pattern

    def check(self, market_id: str, text: str) -> Optional[tuple[str, str]]:
        """
        Check if any keyword for the given market appears in the text.
        
        Returns:
            (matched_keyword, surrounding_context) or None
        """
        pattern = self._compiled_patterns.get(market_id)
        if not pattern:
            return None

        match = pattern.search(text)
        if match:
            keyword = match.group(0)
            # Get surrounding context (50 chars on each side)
            start = max(0, match.start() - 50)
            end = min(len(text), match.end() + 50)
            context = text[start:end].strip()
            return keyword, context

        return None


class TranscriptSniperStrategy:
    """
    Real-time transcript monitoring for Kalshi mention markets.
    
    This is the coordinator that ties together:
    - Audio capture from streams
    - Transcription via Whisper/Deepgram
    - Keyword matching
    - Signal generation
    """

    def __init__(
        self,
        kalshi_exchange=None,
        openai_api_key: str = "",
        enable_whisper: bool = False,
    ):
        self.kalshi = kalshi_exchange
        self._active = False
        self._enable_whisper = enable_whisper and bool(openai_api_key)

        # Components
        self.transcriber = WhisperTranscriber(api_key=openai_api_key)
        self.matcher = KeywordMatcher()

        # Targets
        self.targets: list[MentionTarget] = []

        # Stats
        self._signals: list[Signal] = []
        self._chunks_processed = 0
        self._keywords_detected = 0

    async def start(self):
        self._active = True
        if self._enable_whisper:
            await self.transcriber.start()
        else:
            logger.info("Transcript sniper: Whisper disabled (TRANSCRIPT_ENABLE_WHISPER=false)")
        logger.info("Transcript sniper: started with %d targets", len(self.targets))

    async def stop(self):
        self._active = False
        if self._enable_whisper:
            await self.transcriber.close()

    def add_target(self, target: MentionTarget):
        """Register a mention target to monitor."""
        self.targets.append(target)
        match_key = target.market_ticker or target.kalshi_series
        if target.keywords:
            self.matcher.add_keywords(match_key, target.keywords)
        logger.info(
            "Transcript sniper: added target %s (%d keywords)",
            target.show_name, len(target.keywords),
        )

    async def discover_mention_markets(self) -> list[MentionTarget]:
        """
        Auto-discover mention markets from Kalshi and extract keywords
        from market titles.
        """
        if not self.kalshi:
            return []

        discovered = []
        mention_series = ["KXSNLMENTION", "KXMTPMENTION", "KXTRUMPSAY", "KXMRBEASTMENTION"]

        for series in mention_series:
            try:
                markets = await self.kalshi.fetch_markets(category=series)
                active = [m for m in markets if m.get("status") == "active"]

                for m in active:
                    title = m.get("title", "")
                    ticker = m.get("ticker", "")
                    keywords = self._extract_keywords_from_title(title)
                    if not keywords or not ticker:
                        continue

                    target = MentionTarget(
                        kalshi_series=series,
                        market_ticker=ticker,
                        keywords=list(set(keywords)),
                        stream_url="",
                        show_name=series.replace("KX", "").replace("MENTION", ""),
                    )
                    discovered.append(target)
                    self.add_target(target)

                if active:
                    logger.info("Discovered %s: %d active markets", series, len(active))

            except Exception as e:
                logger.error("Mention market discovery failed for %s: %s", series, e)

        return discovered

    def _extract_keywords_from_title(self, title: str) -> list[str]:
        """
        Extract the mention keyword from a Kalshi market title.
        
        Examples:
          "Will SNL mention Elon Musk?"     → ["Elon Musk"]
          "Will Trump say 'tariff'?"         → ["tariff"]
          "Will MrBeast mention Pewdiepie?" → ["Pewdiepie"]
        """
        patterns = [
            re.compile(r"mention\s+['\"]?(.+?)['\"]?\s*\?", re.IGNORECASE),
            re.compile(r"say\s+['\"](.+?)['\"]", re.IGNORECASE),
            re.compile(r"mention\s+(.+?)(?:\s+on|\s+in|\s+during|\?)", re.IGNORECASE),
        ]

        for pattern in patterns:
            match = pattern.search(title)
            if match:
                keyword = match.group(1).strip().strip("'\"")
                if keyword and len(keyword) > 1:
                    return [keyword]

        return []

    async def process_audio_chunk(self, audio_data: bytes, source_stream: str = "") -> list[Signal]:
        """
        Process a single audio chunk: transcribe → match → signal.
        This is the main entry point for the pipeline.
        """
        signals = []

        # Transcribe
        chunk = await self.transcriber.transcribe_audio(audio_data)
        if not chunk or not chunk.text:
            return signals

        self._chunks_processed += 1

        # Check each target for keyword matches
        for target in self.targets:
            if target.detected:
                continue  # Already detected for this market

            match_key = target.market_ticker or target.kalshi_series
            result = self.matcher.check(match_key, chunk.text)
            if result:
                keyword, context = result
                self._keywords_detected += 1
                target.detected = True

                signal = Signal(
                    strategy="transcript_sniper",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=target.market_ticker or target.kalshi_series,
                    target_price=0.95,
                    quantity=10,
                    confidence=0.95,
                    reason=f'Keyword "{keyword}" detected in {target.show_name}: "...{context}..."'
                )

                signals.append(signal)
                self._signals.append(signal)

                logger.info(
                    "MENTION DETECTED: '%s' in %s",
                    keyword, target.show_name,
                )

        return signals

    async def simulate_transcript(self, text: str) -> list[Signal]:
        """
        Test mode: process a text string directly without audio.
        Useful for testing keyword matching without a live stream.
        """
        signals = []

        for target in self.targets:
            if target.detected:
                continue

            match_key = target.market_ticker or target.kalshi_series
            result = self.matcher.check(match_key, text)
            if result:
                keyword, context = result
                self._keywords_detected += 1
                target.detected = True

                signal = Signal(
                    strategy="transcript_sniper",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=target.market_ticker or target.kalshi_series,
                    target_price=0.95,
                    quantity=10,
                    confidence=0.95,
                    reason=f'Simulated: "{keyword}" found in text'
                )
                signals.append(signal)
                self._signals.append(signal)

        return signals

    def get_stats(self) -> dict:
        return {
            "targets": len(self.targets),
            "chunks_processed": self._chunks_processed,
            "keywords_detected": self._keywords_detected,
            "signals_generated": len(self._signals),
        }
