"""
Strategies V2 — Expansion modules for the Prediction Market Bot.

These strategies are ISOLATED from the main bot (strategies/) and can be
tested independently before being wired into main.py.

Modules:
  - weather_arb:       Hourly temperature/precip arb via NOAA station data
  - macro_news:        CPI / NFP / Fed Decision sniper via government feeds
  - equity_index_arb:  S&P 500 / Nasdaq-100 close prediction arb
  - transcript_sniper: Real-time audio transcription for mention markets
"""
