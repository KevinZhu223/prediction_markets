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

    # ── Risk Parameters ─────────────────────────────────────────────────
    MAX_POSITION_PCT: float = float(os.getenv("MAX_POSITION_PCT", "0.03"))
    MAX_SECTOR_PCT: float = float(os.getenv("MAX_SECTOR_PCT", "0.30"))
    MAX_DRAWDOWN_PCT: float = float(os.getenv("MAX_DRAWDOWN_PCT", "0.15"))
    KELLY_FRACTION: float = float(os.getenv("KELLY_FRACTION", "0.50"))
    MIN_EDGE_THRESHOLD: float = float(os.getenv("MIN_EDGE_THRESHOLD", "0.02"))

    # ── Latency Arb Specific ────────────────────────────────────────────
    SPOT_PRICE_JUMP_THRESHOLD: float = float(os.getenv("SPOT_PRICE_JUMP_THRESHOLD", "50.0"))
    CRYPTO_MARKET_WINDOW_MINUTES: int = int(os.getenv("CRYPTO_MARKET_WINDOW_MINUTES", "5"))

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

    # ── Transcript Sniper ───────────────────────────────────────────────
    TRANSCRIPT_ENABLE_WHISPER: bool = os.getenv("TRANSCRIPT_ENABLE_WHISPER", "false").lower() == "true"

    # ── LLM Linguistic Sniper ───────────────────────────────────────────
    LINGUISTIC_ENABLED: bool = os.getenv("LINGUISTIC_ENABLED", "false").lower() == "true"

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
