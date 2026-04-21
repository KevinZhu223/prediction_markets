"""
Strategies V2 — Expansion modules for the Prediction Market Bot.

These strategies are ISOLATED from the main bot (strategies/) and can be
tested independently before being wired into main.py.

Active Modules:
  - macro_news:        CPI / NFP / Fed Decision sniper via government feeds
  - equity_index_arb:  S&P 500 / Nasdaq-100 close prediction arb
  - transcript_sniper: Real-time audio transcription for mention markets
  - forex_arb:         EUR/USD & USD/JPY hourly directional arb via Yahoo Finance
  - tennis_arb:        ATP/WTA/Challenger match & set winner arb via ESPN live scores
  - yield_farmer:      Near-settlement certainty gap farming
  - longshot_fader:    Favorite-longshot bias fading (buy NO on cheap longshots)

Disabled Modules (set env var to "true" to re-enable):
  - weather_arb:       DISABLED — Kalshi settles on NWS CLI reports, not METAR
                       observations. Our NOAA feed reads METAR which can differ
                       by 1-2°F, causing systematic losses (2/26 win rate).
                       Re-enable via WEATHER_ENABLED=true.
  - commodity_arb:     DISABLED — Lithium (0/3 wins), WTI (marginal). Yahoo
                       Finance data is too stale vs professional desks.
                       Re-enable via COMMODITY_ENABLED=true.
"""
