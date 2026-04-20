"""
Tennis Match Arbitrage Strategy

Exploits the lag between real-time tennis match scores and Kalshi's
match/set winner markets.  When a player has a commanding lead
(e.g., won the first set and is breaking in the second), the probability
of them winning is very high, but Kalshi market prices often lag behind.

How it works:
  1. Kalshi has markets like "Will Djokovic beat Alcaraz?" for ATP,
     WTA and Challenger tennis matches.
  2. ESPN provides free, real-time tennis scoreboard data via public
     JSON endpoints — NO API key required, zero LLM cost.
  3. We monitor live scores and detect momentum shifts:
     - Player won first set → ~67% likely to win match
     - Player won first set AND is up a break → ~80%+
     - Player leads 2-0 in a best-of-3 → ~97%
  4. When Kalshi prices don't reflect these probabilities, we pounce.

Target Kalshi series:
  - KXATPMATCH            : ATP Tour match winners
  - KXATPCHALLENGERMATCH  : ATP Challenger match winners
  - KXWTAMATCH            : WTA Tour match winners
  - KXATPSETWINNER        : ATP set winners

Data source:
  - ESPN Tennis Scoreboard API (free, public, no API key)
    ATP: https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard
    WTA: https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass, field

import httpx

from config import Config
from models import Signal, Side, ContractSide, Platform

logger = logging.getLogger(__name__)


# ── ESPN Endpoints ──────────────────────────────────────────────────────────

ESPN_SCOREBOARDS = {
    "atp": "https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard",
    "wta": "https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard",
}

# Kalshi series prefixes for tennis markets
TENNIS_SERIES = [
    "KXATPMATCH",
    "KXATPCHALLENGERMATCH",
    "KXWTAMATCH",
    "KXATPSETWINNER",
]


# ── Data Models ─────────────────────────────────────────────────────────────

@dataclass
class LiveMatch:
    """A live tennis match parsed from ESPN."""
    player1_name: str
    player2_name: str
    player1_sets: int
    player2_sets: int
    player1_games: int      # Games in the *current* (in-progress) set
    player2_games: int
    is_complete: bool
    winner_name: Optional[str]
    tour: str               # "atp" or "wta"
    best_of: int            # 3 or 5
    player1_last_name: str = ""
    player2_last_name: str = ""
    match_id: str = ""


# ── ESPN Feed ───────────────────────────────────────────────────────────────

class ESPNTennisFeed:
    """Fetches live tennis scores from ESPN's public scoreboard API."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=8.0,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; PredictionBot/1.0)",
                "Accept": "application/json",
            },
        )
        self._cache: list[LiveMatch] = []
        self._cache_ts: float = 0.0
        self._cache_ttl: float = 25.0
        self._last_error_log_ts: dict[str, float] = {}

    def _log_error_throttled(self, key: str, msg: str, interval: float = 120.0):
        now = time.time()
        if (now - self._last_error_log_ts.get(key, 0.0)) >= interval:
            logger.warning(msg)
            self._last_error_log_ts[key] = now
        else:
            logger.debug(msg)

    async def get_live_matches(self) -> list[LiveMatch]:
        """Return all currently live tennis matches (cached)."""
        now = time.time()
        if self._cache and (now - self._cache_ts) < self._cache_ttl:
            return self._cache

        matches: list[LiveMatch] = []

        for tour, url in ESPN_SCOREBOARDS.items():
            fetched = await self._fetch_scoreboard(url, tour)
            matches.extend(fetched)
            # small delay between tours
            await asyncio.sleep(0.4)

        self._cache = matches
        self._cache_ts = time.time()
        return matches

    async def _fetch_scoreboard(self, url: str, tour: str) -> list[LiveMatch]:
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            data = resp.json()

            matches: list[LiveMatch] = []
            for event in data.get("events", []):
                m = self._parse_event(event, tour)
                if m:
                    matches.append(m)
            return matches

        except Exception as e:
            self._log_error_throttled(
                f"espn:{tour}",
                f"ESPN tennis fetch failed ({tour}): {e}",
            )
            return []

    def _parse_event(self, event: dict, tour: str) -> Optional[LiveMatch]:
        try:
            comps = event.get("competitions", [])
            if not comps:
                return None
            comp = comps[0]
            competitors = comp.get("competitors", [])
            if len(competitors) < 2:
                return None

            status = comp.get("status", {}).get("type", {})
            is_complete = status.get("completed", False)
            state = status.get("name", "")

            # Only in-progress or recently completed
            if state not in (
                "STATUS_IN_PROGRESS",
                "STATUS_FINAL",
                "STATUS_END_PERIOD",
            ) and not is_complete:
                return None

            p1 = competitors[0]
            p2 = competitors[1]

            # Player names — ESPN sometimes nests under 'athlete' or 'team'
            p1_name = (
                p1.get("athlete", {}).get("displayName", "")
                or p1.get("team", {}).get("displayName", "")
                or ""
            )
            p2_name = (
                p2.get("athlete", {}).get("displayName", "")
                or p2.get("team", {}).get("displayName", "")
                or ""
            )
            if not p1_name or not p2_name:
                return None

            # Parse set scores from linescores
            p1_lines = p1.get("linescores", [])
            p2_lines = p2.get("linescores", [])

            p1_sets, p2_sets = 0, 0
            p1_cur, p2_cur = 0, 0

            for ls1, ls2 in zip(p1_lines, p2_lines):
                g1 = int(float(ls1.get("value", 0)))
                g2 = int(float(ls2.get("value", 0)))

                set_done = (
                    (g1 >= 6 and g1 - g2 >= 2)
                    or (g2 >= 6 and g2 - g1 >= 2)
                    or (g1 == 7 and g2 == 6)
                    or (g2 == 7 and g1 == 6)
                    or (g1 >= 6 and g2 >= 6 and abs(g1 - g2) >= 2)
                )
                if set_done:
                    if g1 > g2:
                        p1_sets += 1
                    else:
                        p2_sets += 1
                else:
                    # Current (in-progress) set
                    p1_cur = g1
                    p2_cur = g2

            winner_name = None
            if is_complete:
                if p1.get("winner"):
                    winner_name = p1_name
                elif p2.get("winner"):
                    winner_name = p2_name

            # Grand Slams → best-of-5 for ATP
            best_of = 3
            ev_name = event.get("name", "").lower()
            grand_slams = [
                "australian open", "french open", "roland garros",
                "wimbledon", "us open",
            ]
            if tour == "atp" and any(gs in ev_name for gs in grand_slams):
                best_of = 5

            p1_last = p1_name.split()[-1] if p1_name else ""
            p2_last = p2_name.split()[-1] if p2_name else ""

            return LiveMatch(
                player1_name=p1_name,
                player2_name=p2_name,
                player1_sets=p1_sets,
                player2_sets=p2_sets,
                player1_games=p1_cur,
                player2_games=p2_cur,
                is_complete=is_complete,
                winner_name=winner_name,
                tour=tour,
                best_of=best_of,
                player1_last_name=p1_last,
                player2_last_name=p2_last,
                match_id=event.get("id", ""),
            )

        except Exception as e:
            logger.debug("ESPN event parse error: %s", e)
            return None

    async def close(self):
        await self._client.aclose()


# ── Strategy ────────────────────────────────────────────────────────────────

class TennisArbStrategy:
    """
    Monitors live tennis scores from ESPN and generates signals for
    Kalshi match/set winner markets.

    This is a PURE DATA strategy — zero LLM calls, zero OpenAI cost.
    Edge comes from ESPN's near-real-time score data being faster
    than Kalshi's market re-pricing on momentum shifts.
    """

    def __init__(self, kalshi_exchange=None):
        self.kalshi = kalshi_exchange
        self.feed = ESPNTennisFeed()
        self._active = False

        # Configuration (from config.py)
        self._poll_interval = max(20.0, Config.TENNIS_POLL_INTERVAL_SECONDS)
        self._min_edge = max(0.0, Config.TENNIS_MIN_EDGE)
        self._max_spread = max(0.0, Config.TENNIS_MAX_SPREAD)
        self._signal_cooldown = max(30.0, Config.TENNIS_SIGNAL_COOLDOWN_SECONDS)
        self._min_fair_prob = 0.72  # Only trade when model says >= 72% (clear dominance)

        # State
        self._active_markets: list[dict] = []
        self._signals: list[Signal] = []
        self._last_signal_by_market: dict[str, float] = {}
        self._last_discovery_ts = 0.0
        self._discovery_interval = 180.0  # Refresh markets every 3 min
        self._last_discovery_count = -1
        self._last_discovery_log_ts = 0.0

    async def start(self):
        self._active = True
        logger.info(
            "Tennis arb: started (poll=%.0fs, min_edge=%.2f, cooldown=%.0fs)",
            self._poll_interval, self._min_edge, self._signal_cooldown,
        )

    async def stop(self):
        self._active = False
        await self.feed.close()

    async def run_loop(self, signal_callback=None):
        """Main loop: discover markets → poll ESPN → generate signals."""
        while self._active:
            try:
                now = time.time()
                if (
                    not self._active_markets
                    or (now - self._last_discovery_ts) >= self._discovery_interval
                ):
                    await self._discover_markets()
                    self._last_discovery_ts = now

                if not self._active_markets:
                    await asyncio.sleep(self._poll_interval)
                    continue

                # Fetch live scores
                live_matches = await self.feed.get_live_matches()

                if live_matches:
                    for market in self._active_markets:
                        signal = self._evaluate_market(market, live_matches)
                        if signal:
                            self._signals.append(signal)
                            logger.info(
                                "TENNIS SIGNAL: %s %s %s @ %.2f (reason: %s)",
                                signal.action.value,
                                signal.contract_side.value,
                                signal.market_id,
                                signal.target_price,
                                signal.reason,
                            )
                            if signal_callback:
                                await signal_callback(signal)

            except Exception as e:
                logger.error("Tennis arb loop error: %s", e)

            await asyncio.sleep(self._poll_interval)

    # ── Market Discovery ────────────────────────────────────────────────

    async def _discover_markets(self):
        """Find active tennis match markets on Kalshi."""
        if not self.kalshi:
            return

        try:
            active_markets: list[dict] = []
            seen_tickers: set[str] = set()

            for series in TENNIS_SERIES:
                try:
                    markets = await self.kalshi.fetch_markets(category=series)
                    for m in markets:
                        if (m.get("status") or "").lower() not in {"active", "open"}:
                            continue
                        ticker = m.get("ticker", "")
                        if not ticker or ticker in seen_tickers:
                            continue
                        volume = float(m.get("volume_fp", 0) or 0)
                        if volume < 5:
                            continue
                        seen_tickers.add(ticker)
                        active_markets.append(m)
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.debug("Tennis discovery failed for %s: %s", series, e)

            self._active_markets = active_markets

            now = time.time()
            should_log = (
                len(self._active_markets) != self._last_discovery_count
                or (now - self._last_discovery_log_ts) >= 300.0
            )
            if should_log:
                logger.info(
                    "Tennis arb: tracking %d active tennis markets",
                    len(self._active_markets),
                )
                self._last_discovery_count = len(self._active_markets)
                self._last_discovery_log_ts = now

        except Exception as e:
            logger.error("Tennis market discovery failed: %s", e)

    # ── Market Evaluation ───────────────────────────────────────────────

    def _evaluate_market(
        self, market: dict, live_matches: list[LiveMatch]
    ) -> Optional[Signal]:
        """Match a Kalshi market to a live ESPN score and generate a signal."""
        ticker = market.get("ticker", "")
        title = market.get("title", "")

        # Cooldown
        last_ts = self._last_signal_by_market.get(ticker, 0.0)
        if (time.time() - last_ts) < self._signal_cooldown:
            return None

        # Prices
        yes_bid_d = market.get("yes_bid_dollars")
        yes_ask_d = market.get("yes_ask_dollars")
        yes_bid = float(yes_bid_d) if yes_bid_d and float(yes_bid_d) > 0 else 0.0
        yes_ask = float(yes_ask_d) if yes_ask_d and float(yes_ask_d) > 0 else 0.0

        if yes_ask <= 0 and yes_bid <= 0:
            return None

        # Spread filter
        if yes_bid > 0 and yes_ask > 0 and (yes_ask - yes_bid) > self._max_spread:
            return None

        # Parse players
        players = self._extract_players(title, ticker)
        if not players:
            return None
        player_yes, player_no = players

        # Set-winner market?
        is_set_market = "SETWINNER" in ticker.upper()
        set_number = self._extract_set_number(ticker) if is_set_market else None

        # Match to ESPN
        matched = self._find_matching_match(player_yes, player_no, live_matches)
        if not matched:
            return None

        # Skip completed matches (Kalshi should have already settled)
        if matched.is_complete:
            return None

        # Determine orientation: which ESPN player is "YES"
        yes_is_p1 = self._player_matches(
            player_yes, matched.player1_name, matched.player1_last_name
        )

        if yes_is_p1:
            y_sets, n_sets = matched.player1_sets, matched.player2_sets
            y_games, n_games = matched.player1_games, matched.player2_games
        else:
            y_sets, n_sets = matched.player2_sets, matched.player1_sets
            y_games, n_games = matched.player2_games, matched.player1_games

        # Calculate fair probability
        if is_set_market and set_number:
            fair_prob = self._estimate_set_win_probability(y_games, n_games)
        else:
            fair_prob = self._estimate_match_win_probability(
                y_sets, n_sets, y_games, n_games, matched.best_of,
            )

        # Generate signal — only when model is confident enough
        if fair_prob >= self._min_fair_prob and yes_ask > 0:
            edge = fair_prob - yes_ask
            if edge >= self._min_edge:
                self._last_signal_by_market[ticker] = time.time()
                return Signal(
                    strategy="tennis_arb",
                    action=Side.BUY,
                    contract_side=ContractSide.YES,
                    platform=Platform.KALSHI,
                    market_id=ticker,
                    target_price=yes_ask,
                    quantity=self._calculate_size(edge),
                    confidence=min(0.95, fair_prob),
                    reason=(
                        f"Tennis: {player_yes} lead "
                        f"({y_sets}-{n_sets} sets, {y_games}-{n_games} games) "
                        f"fair={fair_prob:.2f} vs ask={yes_ask:.2f} edge={edge:.2f}"
                    ),
                )

        if fair_prob <= (1.0 - self._min_fair_prob) and yes_bid > 0:
            no_fair = 1.0 - fair_prob
            no_ask = 1.0 - yes_bid
            if no_ask > 0:
                edge = no_fair - no_ask
                if edge >= self._min_edge:
                    self._last_signal_by_market[ticker] = time.time()
                    return Signal(
                        strategy="tennis_arb",
                        action=Side.BUY,
                        contract_side=ContractSide.NO,
                        platform=Platform.KALSHI,
                        market_id=ticker,
                        target_price=no_ask,
                        quantity=self._calculate_size(edge),
                        confidence=min(0.95, no_fair),
                        reason=(
                            f"Tennis: {player_no} lead "
                            f"({n_sets}-{y_sets} sets, {n_games}-{y_games} games) "
                            f"fair_no={no_fair:.2f} vs no_ask={no_ask:.2f} edge={edge:.2f}"
                        ),
                    )

        return None

    # ── Probability Models ──────────────────────────────────────────────

    def _estimate_match_win_probability(
        self,
        sets_won: int,
        sets_lost: int,
        games_won: int,
        games_lost: int,
        best_of: int = 3,
    ) -> float:
        """
        Estimate P(win match) based on current score state.
        Data-calibrated against historical ATP/WTA match outcomes.

        Key insight: in best-of-3, winning the first set gives ~67%
        win probability.  Up a break in the second set pushes it to ~80%+.
        """
        sets_needed = (best_of + 1) // 2

        # Clinched
        if sets_won >= sets_needed:
            return 0.97
        if sets_lost >= sets_needed:
            return 0.03

        # Base probability table (sets_won, sets_lost) → P(win)
        if best_of == 3:
            base_table = {
                (0, 0): 0.50,
                (1, 0): 0.67,
                (0, 1): 0.33,
                (1, 1): 0.50,
            }
        else:  # best-of-5
            base_table = {
                (0, 0): 0.50,
                (1, 0): 0.61,
                (0, 1): 0.39,
                (1, 1): 0.50,
                (2, 0): 0.83,
                (0, 2): 0.17,
                (2, 1): 0.67,
                (1, 2): 0.33,
                (2, 2): 0.50,
            }

        base = base_table.get((sets_won, sets_lost), 0.50)

        # Adjust for current-set game score
        game_diff = games_won - games_lost
        if game_diff != 0:
            # Each game of lead ≈ 2.5% probability shift
            adj = max(-0.15, min(0.15, game_diff * 0.025))
            base = max(0.03, min(0.97, base + adj))

        return base

    @staticmethod
    def _estimate_set_win_probability(
        games_won: int, games_lost: int,
    ) -> float:
        """Estimate P(winning the current set) from in-set game score."""
        # Already won/lost this set
        if games_won >= 6 and games_won - games_lost >= 2:
            return 0.97
        if games_lost >= 6 and games_lost - games_won >= 2:
            return 0.03
        if games_won == 7:
            return 0.97
        if games_lost == 7:
            return 0.03

        game_diff = games_won - games_lost

        # Each game of lead ≈ 8% shift; amplify near set point
        base = 0.50 + game_diff * 0.08

        # Bonus when serving for the set
        if games_won >= 5 and games_lost <= 4:
            base = min(0.95, base + 0.10)
        elif games_lost >= 5 and games_won <= 4:
            base = max(0.05, base - 0.10)

        return max(0.03, min(0.97, base))

    # ── Player Name Matching ────────────────────────────────────────────

    def _extract_players(
        self, title: str, ticker: str,
    ) -> Optional[tuple[str, str]]:
        """
        Extract (player_YES, player_NO) from a Kalshi market title/ticker.

        Titles look like:
          "Will Djokovic beat Alcaraz?"
          "Will Cobb beat Dedecker in the ATP…?"
          "Who will win Set 1: Nagai vs Cerovac?"
        """
        # Pattern 1: "Will X beat Y"
        m = re.search(
            r'[Ww]ill\s+(.+?)\s+beat\s+(.+?)(?:\s+in\s|\s*\?)',
            title,
        )
        if m:
            return m.group(1).strip(), m.group(2).strip()

        # Pattern 2: "Who will win ... X vs Y"
        m = re.search(
            r'(?:win|winner).*?:\s*(.+?)\s+vs\.?\s+(.+?)(?:\s*\?|$)',
            title,
            re.IGNORECASE,
        )
        if m:
            p1, p2 = m.group(1).strip(), m.group(2).strip()
            # Determine YES side from ticker suffix
            winner_code = ticker.rsplit("-", 1)[-1] if "-" in ticker else ""
            if winner_code and len(winner_code) >= 3:
                p1_code = p1[:3].upper()
                p2_code = p2[:3].upper()
                if winner_code.upper().startswith(p1_code):
                    return p1, p2
                elif winner_code.upper().startswith(p2_code):
                    return p2, p1
            return p1, p2

        # Pattern 3: Ticker anatomy — e.g. KXATPMATCH-26APR14COBDED-COB
        m = re.search(r'([A-Z]{3})([A-Z]{3})-([A-Z]{3})$', ticker)
        if m:
            p1_code, p2_code, win_code = m.group(1), m.group(2), m.group(3)
            if win_code == p1_code:
                return p1_code, p2_code
            return p2_code, p1_code

        return None

    @staticmethod
    def _extract_set_number(ticker: str) -> Optional[int]:
        """Extract set number from KXATPSETWINNER-...-1-CER."""
        m = re.search(r'-(\d+)-[A-Z]+$', ticker)
        return int(m.group(1)) if m else None

    def _find_matching_match(
        self,
        player_yes: str,
        player_no: str,
        live_matches: list[LiveMatch],
    ) -> Optional[LiveMatch]:
        """Find the ESPN LiveMatch that corresponds to the two Kalshi players."""
        for match in live_matches:
            p1y = self._player_matches(
                player_yes, match.player1_name, match.player1_last_name,
            )
            p2n = self._player_matches(
                player_no, match.player2_name, match.player2_last_name,
            )
            p1n = self._player_matches(
                player_no, match.player1_name, match.player1_last_name,
            )
            p2y = self._player_matches(
                player_yes, match.player2_name, match.player2_last_name,
            )

            if (p1y and p2n) or (p2y and p1n):
                return match
        return None

    @staticmethod
    def _player_matches(
        kalshi_name: str, espn_full: str, espn_last: str,
    ) -> bool:
        """Fuzzy-match a Kalshi player name to an ESPN player name."""
        kl = kalshi_name.lower().strip()
        ef = espn_full.lower().strip()
        el = espn_last.lower().strip()

        # Exact full match
        if kl == ef:
            return True

        # Last-name match
        k_parts = kl.split()
        if k_parts:
            k_last = k_parts[-1]
            if k_last == el and len(k_last) >= 3:
                return True
            # 3-letter abbreviation
            if len(kl) == 3 and el.startswith(kl):
                return True

        # Substring containment (≥4 chars to avoid false positives)
        if len(kl) >= 4 and kl in ef:
            return True
        if len(el) >= 4 and el in kl:
            return True

        return False

    # ── Sizing ──────────────────────────────────────────────────────────

    @staticmethod
    def _calculate_size(edge: float) -> int:
        """Conservative position sizing based on edge magnitude."""
        if edge >= 0.20:
            return 30
        elif edge >= 0.15:
            return 25
        elif edge >= 0.10:
            return 20
        return 15

    # ── Stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        return {
            "active_markets": len(self._active_markets),
            "signals_generated": len(self._signals),
        }
