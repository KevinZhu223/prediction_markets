"""
Prediction Market Exploitation Bot - Configuration
Loads environment variables and provides typed config access.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Centralized configuration loaded from environment variables."""

    # ── Kalshi ──────────────────────────────────────────────────────────
    # Elections domain (Politics, Culture, Sports)
    KALSHI_API_KEY: str = os.getenv("KALSHI_API_KEY", "")
    KALSHI_PRIVATE_KEY_PATH: str = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./keys/kalshi_private.pem")
    KALSHI_BASE_URL: str = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com")
    KALSHI_WS_URL: str = os.getenv("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")

    # Standard domain (Weather, Financials, Economics)
    KALSHI_STANDARD_API_KEY: str = os.getenv("KALSHI_STANDARD_API_KEY", "")
    KALSHI_STANDARD_PRIVATE_KEY_PATH: str = os.getenv("KALSHI_STANDARD_PRIVATE_KEY_PATH", "./keys/kalshi_standard_private.pem")
    KALSHI_STANDARD_BASE_URL: str = "https://trading-api.kalshi.com"

    # ── OpenAI ──────────────────────────────────────────────────────────
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    OPENAI_MAX_OUTPUT_TOKENS: int = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "180"))
    OPENAI_DAILY_TOKEN_BUDGET: int = int(os.getenv("OPENAI_DAILY_TOKEN_BUDGET", "120000"))

    # ── Bot Mode ────────────────────────────────────────────────────────
    PAPER_TRADING: bool = os.getenv("PAPER_TRADING", "true").lower() == "true"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    SESSION_TAKEOVER_EXISTING: bool = os.getenv("SESSION_TAKEOVER_EXISTING", "true").lower() == "true"
    STARTUP_STAGGER_SECONDS: float = float(os.getenv("STARTUP_STAGGER_SECONDS", "0.6"))

    # ── Data Freshness / Throughput Guards ──────────────────────────────
    ORDERBOOK_MAX_AGE_SECONDS: float = float(os.getenv("ORDERBOOK_MAX_AGE_SECONDS", "6.0"))
    SPOT_PRICE_MAX_AGE_SECONDS: float = float(os.getenv("SPOT_PRICE_MAX_AGE_SECONDS", "8.0"))
    SIGNAL_MAX_AGE_SECONDS: float = float(os.getenv("SIGNAL_MAX_AGE_SECONDS", "20.0"))
    EXECUTOR_QUEUE_MAX_SIZE: int = int(os.getenv("EXECUTOR_QUEUE_MAX_SIZE", "800"))
    EXECUTOR_QUEUE_WARN_THRESHOLD: int = int(os.getenv("EXECUTOR_QUEUE_WARN_THRESHOLD", "250"))
    EXECUTOR_DROP_WHEN_FULL: bool = os.getenv("EXECUTOR_DROP_WHEN_FULL", "true").lower() == "true"
    PENDING_SIGNAL_TTL_SECONDS: float = float(os.getenv("PENDING_SIGNAL_TTL_SECONDS", "120.0"))
    SIGNAL_DUPLICATE_COOLDOWN_SECONDS: float = float(
        os.getenv("SIGNAL_DUPLICATE_COOLDOWN_SECONDS", "45.0")
    )
    SIGNAL_DUPLICATE_PRICE_TOLERANCE: float = float(
        os.getenv("SIGNAL_DUPLICATE_PRICE_TOLERANCE", "0.015")
    )

    # ── Risk Parameters ─────────────────────────────────────────────────
    MAX_POSITION_PCT: float = float(os.getenv("MAX_POSITION_PCT", "0.05"))
    MAX_SECTOR_PCT: float = float(os.getenv("MAX_SECTOR_PCT", "0.30"))
    MAX_DRAWDOWN_PCT: float = float(os.getenv("MAX_DRAWDOWN_PCT", "0.15"))
    KELLY_FRACTION: float = float(os.getenv("KELLY_FRACTION", "0.20"))
    MIN_SIGNAL_CONFIDENCE: float = float(os.getenv("MIN_SIGNAL_CONFIDENCE", "0.20"))
    RISK_TEST_MODE: bool = os.getenv("RISK_TEST_MODE", "true").lower() == "true"
    TEST_MAX_POSITION_PCT: float = float(os.getenv("TEST_MAX_POSITION_PCT", "0.10"))
    TEST_MAX_DRAWDOWN_PCT: float = float(os.getenv("TEST_MAX_DRAWDOWN_PCT", "0.35"))
    TEST_KELLY_FRACTION: float = float(os.getenv("TEST_KELLY_FRACTION", "0.75"))
    TRADE_JOURNAL_ENABLED: bool = os.getenv("TRADE_JOURNAL_ENABLED", "true").lower() == "true"
    MIN_EDGE_THRESHOLD: float = float(os.getenv("MIN_EDGE_THRESHOLD", "0.02"))
    KELLY_ONE_LOT_CONFIDENCE_FLOOR: float = float(
        os.getenv("KELLY_ONE_LOT_CONFIDENCE_FLOOR", "0.70")
    )
    KELLY_ONE_LOT_EDGE_FLOOR: float = float(
        os.getenv("KELLY_ONE_LOT_EDGE_FLOOR", "0.05")
    )

    # ── Latency Arb Specific ────────────────────────────────────────────
    SPOT_PRICE_JUMP_THRESHOLD: float = float(os.getenv("SPOT_PRICE_JUMP_THRESHOLD", "50.0"))
    CRYPTO_MARKET_WINDOW_MINUTES: int = int(os.getenv("CRYPTO_MARKET_WINDOW_MINUTES", "5"))
    LATENCY_MIN_ENTRY_PRICE: float = float(os.getenv("LATENCY_MIN_ENTRY_PRICE", "0.02"))
    LATENCY_MAX_ENTRY_PRICE: float = float(os.getenv("LATENCY_MAX_ENTRY_PRICE", "0.95"))

    # ── AI Market Scanner ───────────────────────────────────────────────
    SCANNER_ENABLED: bool = os.getenv("SCANNER_ENABLED", "true").lower() == "true"
    SCANNER_MODE: str = os.getenv("SCANNER_MODE", "off").strip().lower()  # off | llm
    SCANNER_PESSIMISTIC_MODE: bool = os.getenv("SCANNER_PESSIMISTIC_MODE", "true").lower() == "true"
    SCANNER_MIN_EDGE: float = float(os.getenv("SCANNER_MIN_EDGE", "0.10"))
    SCANNER_INTERVAL_SECONDS: float = float(os.getenv("SCANNER_INTERVAL_SECONDS", "300.0"))
    SCANNER_MAX_MARKETS_PER_SCAN: int = int(os.getenv("SCANNER_MAX_MARKETS_PER_SCAN", "3"))
    SCANNER_CACHE_TTL_SECONDS: int = int(os.getenv("SCANNER_CACHE_TTL_SECONDS", "1800"))
    SCANNER_MARKET_COOLDOWN_SECONDS: int = int(os.getenv("SCANNER_MARKET_COOLDOWN_SECONDS", "21600"))
    SCANNER_MAX_CALLS_PER_HOUR: int = int(os.getenv("SCANNER_MAX_CALLS_PER_HOUR", "24"))
    SCANNER_MIN_VOLUME: float = float(os.getenv("SCANNER_MIN_VOLUME", "25"))

    # ── Yield Farmer ───────────────────────────────────────────────────
    YIELD_MIN_VOLUME: float = float(os.getenv("YIELD_MIN_VOLUME", "500"))
    YIELD_MAX_SPREAD: float = float(os.getenv("YIELD_MAX_SPREAD", "0.08"))
    YIELD_MAX_HOURS_TO_EXPIRY: float = float(os.getenv("YIELD_MAX_HOURS_TO_EXPIRY", "36.0"))

    # ── Weather / Equity Strategy Guards ───────────────────────────────
    WEATHER_OBSERVATION_MAX_AGE_SECONDS: float = float(
        os.getenv("WEATHER_OBSERVATION_MAX_AGE_SECONDS", "1500.0")
    )
    WEATHER_OBS_CACHE_TTL_SECONDS: float = float(
        os.getenv("WEATHER_OBS_CACHE_TTL_SECONDS", "45.0")
    )
    WEATHER_MAX_MARKETS_PER_SCAN: int = int(
        os.getenv("WEATHER_MAX_MARKETS_PER_SCAN", "60")
    )
    WEATHER_DISCOVERY_LOG_COOLDOWN_SECONDS: float = float(
        os.getenv("WEATHER_DISCOVERY_LOG_COOLDOWN_SECONDS", "300.0")
    )
    WEATHER_SIGNAL_COOLDOWN_SECONDS: float = float(
        os.getenv("WEATHER_SIGNAL_COOLDOWN_SECONDS", "900.0")
    )
    WEATHER_MAX_SPREAD: float = float(os.getenv("WEATHER_MAX_SPREAD", "0.14"))
    WEATHER_MAX_HOURS_TO_EXPIRY: float = float(os.getenv("WEATHER_MAX_HOURS_TO_EXPIRY", "6.0"))
    EQUITY_SIGNAL_COOLDOWN_SECONDS: float = float(
        os.getenv("EQUITY_SIGNAL_COOLDOWN_SECONDS", "240.0")
    )
    EQUITY_MARKET_REFRESH_SECONDS: float = float(
        os.getenv("EQUITY_MARKET_REFRESH_SECONDS", "900.0")
    )
    EQUITY_MAX_SPREAD: float = float(os.getenv("EQUITY_MAX_SPREAD", "0.12"))

    # ── Transcript Sniper ───────────────────────────────────────────────
    TRANSCRIPT_ENABLE_WHISPER: bool = os.getenv("TRANSCRIPT_ENABLE_WHISPER", "false").lower() == "true"

    # ── LLM Linguistic Sniper ───────────────────────────────────────────
    LINGUISTIC_ENABLED: bool = os.getenv("LINGUISTIC_ENABLED", "false").lower() == "true"

    # ── API Rate Limiting / Backoff ─────────────────────────────────────
    KALSHI_MAX_CONCURRENT_REQUESTS: int = int(os.getenv("KALSHI_MAX_CONCURRENT_REQUESTS", "8"))
    KALSHI_HTTP_MAX_RETRIES: int = int(os.getenv("KALSHI_HTTP_MAX_RETRIES", "4"))
    KALSHI_HTTP_BACKOFF_BASE_SECONDS: float = float(os.getenv("KALSHI_HTTP_BACKOFF_BASE_SECONDS", "0.35"))
    KALSHI_HTTP_BACKOFF_MAX_SECONDS: float = float(os.getenv("KALSHI_HTTP_BACKOFF_MAX_SECONDS", "4.0"))
    KALSHI_HTTP_JITTER_SECONDS: float = float(os.getenv("KALSHI_HTTP_JITTER_SECONDS", "0.15"))
    KALSHI_MARKETS_PAGE_DELAY_SECONDS: float = float(os.getenv("KALSHI_MARKETS_PAGE_DELAY_SECONDS", "0.3"))

    # ── Transcript Discovery Controls ───────────────────────────────────
    TRANSCRIPT_DISCOVERY_COOLDOWN_SECONDS: float = float(
        os.getenv("TRANSCRIPT_DISCOVERY_COOLDOWN_SECONDS", "900.0")
    )
    TRANSCRIPT_DISCOVERY_SERIES_DELAY_SECONDS: float = float(
        os.getenv("TRANSCRIPT_DISCOVERY_SERIES_DELAY_SECONDS", "0.75")
    )

    @classmethod
    def validate(cls) -> list[str]:
        """Return a list of missing critical config values."""
        warnings = []
        if not cls.KALSHI_API_KEY:
            warnings.append("KALSHI_API_KEY is not set")
        openai_required = (
            (cls.SCANNER_ENABLED and cls.SCANNER_MODE == "llm")
            or cls.TRANSCRIPT_ENABLE_WHISPER
        )
        if openai_required and not cls.OPENAI_API_KEY:
            warnings.append("OPENAI_API_KEY is not set")
        if not Path(cls.KALSHI_PRIVATE_KEY_PATH).exists():
            warnings.append(f"Kalshi private key not found at {cls.KALSHI_PRIVATE_KEY_PATH}")
        return warnings
