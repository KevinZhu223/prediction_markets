"""
Kalshi Exchange Wrapper
Handles RSA-PSS authentication, REST order management,
and WebSocket streaming for Kalshi's CFTC-regulated prediction market.
"""

import asyncio
import base64
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Optional, Callable, Awaitable

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import Config
from models import (
    OrderBook, OrderBookLevel, Order, Fill,
    Side, OrderType, Platform, ContractSide
)
from exchanges.base import ExchangeBase

logger = logging.getLogger(__name__)


class KalshiExchange(ExchangeBase):
    """Kalshi API wrapper with RSA-PSS signing."""

    platform = Platform.KALSHI

    def __init__(self, platform=Platform.KALSHI, api_key: str = None, base_url: str = None, ws_url: str = None, private_key_path: str = None):
        self.platform = platform
        self._api_key = api_key or Config.KALSHI_API_KEY
        self._base_url = base_url or Config.KALSHI_BASE_URL
        self._ws_url = ws_url or Config.KALSHI_WS_URL
        self._private_key_path = private_key_path or Config.KALSHI_PRIVATE_KEY_PATH
        self._private_key = None
        self._client: Optional[httpx.AsyncClient] = None

    # ── Auth helpers ────────────────────────────────────────────────────

    def _load_private_key(self):
        key_path = Path(self._private_key_path)
        if not key_path.exists():
            raise FileNotFoundError(f"Kalshi private key not found at {key_path}")
        with open(key_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(f.read(), password=None)

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        """Create RSA-PSS signature for Kalshi API authentication."""
        # V2 Docs: SIGNATURE_KEY = TIMESTAMP + METHOD + PATH
        # PATH should be the clean path without query params
        clean_path = path.split("?")[0]
        message = f"{timestamp_ms}{method}{clean_path}".encode("utf-8")
        
        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _auth_headers(self, method: str, path: str) -> dict:
        """Construct authentication headers."""
        ts = str(int(time.time() * 1000))
        sig = self._sign(ts, method, path)
        return {
            "KALSHI-ACCESS-KEY": self._api_key.strip(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "Content-Type": "application/json",
        }

    # ── Lifecycle ───────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._load_private_key()
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=10.0)
        logger.info("Kalshi: connected (paper=%s)", Config.PAPER_TRADING)

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
        logger.info("Kalshi: disconnected")

    # ── REST helpers ────────────────────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Perform a signed GET request."""
        headers = self._auth_headers("GET", path)
        resp = await self._client.get(path, headers=headers, params=params)
        
        if resp.status_code != 200:
            logger.error("Kalshi GET %s failed (%d): %s", path, resp.status_code, resp.text)
            resp.raise_for_status()
            
        return resp.json()

    async def _post(self, path: str, body: dict) -> dict:
        """Perform a signed POST request."""
        headers = self._auth_headers("POST", path)
        resp = await self._client.post(path, headers=headers, json=body)
        
        if resp.status_code not in (200, 201):
            logger.error("Kalshi POST %s failed (%d): %s", path, resp.status_code, resp.text)
            resp.raise_for_status()
            
        return resp.json()

    async def _delete(self, path: str) -> dict:
        headers = self._auth_headers("DELETE", path)
        resp = await self._client.delete(path, headers=headers)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _normalize_ladder(raw_levels: list, cents: bool = False) -> list[tuple[float, int]]:
        """Parse raw book levels into (price, quantity) sorted by best bid first."""
        levels: list[tuple[float, int]] = []
        scale = 0.01 if cents else 1.0

        for lvl in raw_levels or []:
            try:
                if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                    continue
                price = float(lvl[0]) * scale
                qty = int(float(lvl[1]))
                if qty <= 0:
                    continue
                if price < 0.0 or price > 1.0:
                    continue
                levels.append((price, qty))
            except (TypeError, ValueError):
                continue

        levels.sort(key=lambda x: -x[0])
        return levels

    @staticmethod
    def _complement_ladder(levels: list[tuple[float, int]]) -> list[tuple[float, int]]:
        """Convert NO-side bid ladder into YES-side ask ladder (and vice versa)."""
        complemented: list[tuple[float, int]] = []
        for price, qty in levels:
            comp = 1.0 - float(price)
            if 0.0 <= comp <= 1.0 and qty > 0:
                complemented.append((comp, qty))
        complemented.sort(key=lambda x: x[0])
        return complemented

    def _extract_bid_ladders(self, data: dict) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
        """
        Return (yes_bid_ladder, no_bid_ladder) where each ladder is sorted best bid first.
        Kalshi orderbooks expose bid ladders per side; asks are complements of the opposite side bids.
        """
        ob_fp = data.get("orderbook_fp")
        if ob_fp:
            yes_bids = self._normalize_ladder(ob_fp.get("yes_dollars", []), cents=False)
            no_bids = self._normalize_ladder(ob_fp.get("no_dollars", []), cents=False)
            return yes_bids, no_bids

        ob = data.get("orderbook", {})
        yes_bids = self._normalize_ladder(ob.get("yes", []), cents=True)
        no_bids = self._normalize_ladder(ob.get("no", []), cents=True)
        return yes_bids, no_bids

    async def _fetch_orderbook_snapshot_for_paper(self, market_id: str) -> dict:
        """Fetch orderbook data for paper fill simulation; prefer signed API then fall back to public GET."""
        path = f"/trade-api/v2/markets/{market_id}/orderbook"

        if self._client and self._private_key and self._api_key:
            try:
                return await self._get(path)
            except Exception as e:
                logger.debug("Paper simulation: signed orderbook fetch failed for %s: %s", market_id, e)

        if self._client:
            resp = await self._client.get(path)
            if resp.status_code != 200:
                resp.raise_for_status()
            return resp.json()

        async with httpx.AsyncClient(base_url=self._base_url, timeout=10.0) as client:
            resp = await client.get(path)
            if resp.status_code != 200:
                resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _vwap_for_quantity(levels: list[tuple[float, int]], quantity: int) -> Optional[float]:
        """Compute VWAP for the requested quantity using price-priority levels."""
        if quantity <= 0:
            return None

        remaining = quantity
        notional = 0.0
        for price, available in levels:
            take = min(remaining, int(available))
            if take <= 0:
                continue
            notional += float(price) * take
            remaining -= take
            if remaining == 0:
                break

        if remaining > 0:
            return None
        return notional / quantity

    async def _simulate_paper_fill(self, order: Order) -> Optional[Fill]:
        """Simulate realistic FOK fills in paper mode using visible orderbook depth."""
        try:
            data = await self._fetch_orderbook_snapshot_for_paper(order.market_id)
        except Exception as e:
            logger.error("Paper simulation: failed to fetch orderbook for %s: %s", order.market_id, e)
            return None

        yes_bids, no_bids = self._extract_bid_ladders(data)

        if order.contract_side == ContractSide.YES:
            if order.side == Side.BUY:
                # Buy YES by lifting YES asks, which are complements of NO bids.
                price_levels = self._complement_ladder(no_bids)
                eligible = [(p, q) for p, q in price_levels if p <= order.price + 1e-9]
            else:
                # Sell YES by hitting YES bids.
                price_levels = yes_bids
                eligible = [(p, q) for p, q in price_levels if p >= order.price - 1e-9]
        else:
            if order.side == Side.BUY:
                # Buy NO by lifting NO asks, which are complements of YES bids.
                price_levels = self._complement_ladder(yes_bids)
                eligible = [(p, q) for p, q in price_levels if p <= order.price + 1e-9]
            else:
                # Sell NO by hitting NO bids.
                price_levels = no_bids
                eligible = [(p, q) for p, q in price_levels if p >= order.price - 1e-9]

        available_qty = sum(q for _, q in eligible)
        if available_qty < order.quantity:
            return None

        fill_price = self._vwap_for_quantity(eligible, order.quantity)
        if fill_price is None:
            return None

        return Fill(
            order=order,
            fill_price=fill_price,
            fill_quantity=order.quantity,
            fees=self._estimate_taker_fee(fill_price, order.quantity),
        )

    # ── Public interface ────────────────────────────────────────────────

    async def fetch_balance(self) -> float:
        data = await self._get("/trade-api/v2/portfolio/balance")
        # Kalshi returns balance in cents
        return data.get("balance", 0) / 100.0

    async def fetch_order_book(self, market_id: str) -> OrderBook:
        path = f"/trade-api/v2/markets/{market_id}/orderbook"
        data = await self._get(path)

        # Kalshi exposes bid ladders by side. For a YES-centric OrderBook:
        # - bids = YES bids
        # - asks = complements of NO bids
        yes_bids, no_bids = self._extract_bid_ladders(data)

        bids = [OrderBookLevel(price=p, quantity=q) for p, q in yes_bids]
        asks = [OrderBookLevel(price=p, quantity=q) for p, q in self._complement_ladder(no_bids)]

        return OrderBook(
            market_id=market_id,
            platform=Platform.KALSHI,
            bids=sorted(bids, key=lambda x: -x.price),
            asks=sorted(asks, key=lambda x: x.price),
        )

    async def place_order(self, order: Order) -> Optional[Fill]:
        if Config.PAPER_TRADING:
            logger.info(
                "[PAPER] Kalshi order: %s %s %s @ %.4f x%d",
                order.side.value,
                order.contract_side.value,
                order.market_id,
                order.price,
                order.quantity,
            )
            fill = await self._simulate_paper_fill(order)
            if not fill:
                logger.info(
                    "[PAPER] Kalshi FOK miss: %s %s %s @ %.4f x%d",
                    order.side.value,
                    order.contract_side.value,
                    order.market_id,
                    order.price,
                    order.quantity,
                )
            return fill

        path = "/trade-api/v2/portfolio/orders"
        body = {
            "ticker": order.market_id,
            "action": "buy" if order.side == Side.BUY else "sell",
            "type": "limit",
            "side": order.contract_side.value,
            "count": order.quantity,
            "client_order_id": order.client_order_id,
        }
        
        target_field = "yes_price" if order.contract_side == ContractSide.YES else "no_price"
        body[target_field] = int(order.price * 100)

        # FOK semantics
        if order.order_type == OrderType.FOK:
            body["time_in_force"] = "fok"

        data = await self._post(path, body)
        resp_order = data.get("order", {})

        if resp_order.get("status") == "filled":
            return Fill(
                order=order,
                fill_price=resp_order.get("avg_price", order.price * 100) / 100.0,
                fill_quantity=resp_order.get("filled_count", order.quantity),
                fees=resp_order.get("fees", 0) / 100.0,
            )
        return None

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await self._delete(f"/trade-api/v2/portfolio/orders/{order_id}")
            return True
        except httpx.HTTPStatusError:
            return False

    async def fetch_markets(self, category: Optional[str] = None, include_mve: bool = False) -> list[dict]:
        params = {"limit": 200, "status": "open"}
        if category:
            params["series_ticker"] = category
        if not include_mve:
            params["mve_filter"] = "exclude"

        all_markets = []
        cursor = None

        # Paginate to get diverse markets (up to 3 pages)
        for _ in range(3):
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/trade-api/v2/markets", params=params)
            page = data.get("markets", [])
            all_markets.extend(page)
            cursor = data.get("cursor")
            if not cursor or not page:
                break

        return all_markets

    async def get_market(self, ticker: str) -> Optional[dict]:
        """Fetch a single market by ticker to check its status."""
        path = f"/trade-api/v2/markets/{ticker}"
        last_error: Optional[Exception] = None

        for attempt in range(3):
            try:
                data = await self._get(path)
                return data.get("market", data)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return None

                # Retry transient server/rate-limit failures.
                if e.response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    last_error = e
                    continue
                raise
            except Exception as e:
                # Network flaps (DNS/connectivity) should be retried before surfacing.
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    last_error = e
                    continue
                raise

        if last_error is not None:
            raise last_error
        return None

    async def get_active_series_ticker(self, series_ticker: str) -> Optional[str]:
        """
        Find the most relevant active ticker for a recurring series (e.g. KXBTC15M).
        Returns the ticker that expires soonest in the future.
        """
        markets = await self.fetch_markets(category=series_ticker)
        if not markets:
            # Try without strict category if not found
            all_markets = await self.fetch_markets()
            markets = [m for m in all_markets if series_ticker in m.get("ticker", "")]

        if not markets:
            return None

        # Filter for future expirations and sort by time
        now_ts = time.time()
        valid = []
        for m in markets:
            exp_str = m.get("expiration_time", "")
            if not exp_str:
                continue
            try:
                # Kalshi uses ISO format for expiration
                import datetime
                exp_dt = datetime.datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
                exp_ts = exp_dt.timestamp()
                if exp_ts > now_ts:
                    valid.append((exp_ts, m.get("ticker")))
            except Exception:
                continue

        if not valid:
            return None

        # Return the one expiring soonest
        valid.sort()
        return valid[0][1]

    async def stream_order_book(
        self,
        market_ids: list[str],
        callback: Callable[[OrderBook], Awaitable[None]],
    ) -> None:
        """Stream live order book updates via Kalshi WebSocket."""
        ts = str(int(time.time() * 1000))
        path = "/trade-api/ws/v2"
        sig = self._sign(ts, "GET", path)

        ws_url = (
            f"{self._ws_url}"
            f"?api_key={self._api_key}"
            f"&timestamp={ts}"
            f"&signature={sig}"
        )

        async for ws in websockets.connect(ws_url, ping_interval=30):
            try:
                # Subscribe to orderbook channels
                for mid in market_ids:
                    sub_msg = {
                        "id": str(uuid.uuid4()),
                        "cmd": "subscribe",
                        "params": {"channels": ["orderbook_delta"], "market_tickers": [mid]},
                    }
                    await ws.send(json.dumps(sub_msg))

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") == "orderbook_snapshot" or msg.get("type") == "orderbook_delta":
                        ob = self._parse_ws_orderbook(msg)
                        if ob:
                            await callback(ob)
            except websockets.ConnectionClosed:
                logger.warning("Kalshi WS disconnected, reconnecting...")
                continue

    # ── Fee estimation ──────────────────────────────────────────────────

    @staticmethod
    def _estimate_taker_fee(price: float, count: int) -> float:
        """
        Kalshi taker fee: ceil(0.07 * C * P * (1 - P))
        price is in 0-1 range.
        """
        import math
        return math.ceil(0.07 * count * price * (1 - price) * 100) / 100

    @staticmethod
    def _estimate_maker_fee(price: float, count: int) -> float:
        import math
        return math.ceil(0.0175 * count * price * (1 - price) * 100) / 100

    # ── WS parsing ──────────────────────────────────────────────────────

    def _parse_ws_orderbook(self, msg: dict) -> Optional[OrderBook]:
        try:
            market_id = msg.get("msg", {}).get("market_ticker", "")
            ob_data = msg.get("msg", {})
            yes_bids = self._normalize_ladder(
                [[p, q] for p, q in zip(ob_data.get("yes", []), ob_data.get("yes_quantities", []))],
                cents=True,
            )
            no_bids = self._normalize_ladder(
                [[p, q] for p, q in zip(ob_data.get("no", []), ob_data.get("no_quantities", []))],
                cents=True,
            )

            bids = [OrderBookLevel(price=p, quantity=q) for p, q in yes_bids]
            asks = [OrderBookLevel(price=p, quantity=q) for p, q in self._complement_ladder(no_bids)]
            return OrderBook(
                market_id=market_id,
                platform=Platform.KALSHI,
                bids=sorted(bids, key=lambda x: -x.price),
                asks=sorted(asks, key=lambda x: x.price),
            )
        except Exception as e:
            logger.error("Failed to parse Kalshi WS orderbook: %s", e)
            return None
