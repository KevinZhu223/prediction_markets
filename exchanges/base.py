"""
Abstract base class for exchange wrappers.
Every platform-specific wrapper must implement this interface.
"""

from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable
from models import OrderBook, Order, Fill, Platform


class ExchangeBase(ABC):
    """Unified interface for prediction market exchanges."""

    platform: Platform

    @abstractmethod
    async def connect(self) -> None:
        """Establish session / authenticate with the exchange."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close connections."""
        ...

    @abstractmethod
    async def fetch_balance(self) -> float:
        """Return available balance in USD (or USDC equivalent)."""
        ...

    @abstractmethod
    async def fetch_order_book(self, market_id: str) -> OrderBook:
        """Fetch the current order book for a given market."""
        ...

    @abstractmethod
    async def place_order(self, order: Order) -> Optional[Fill]:
        """
        Place an order on the exchange.
        Returns a Fill if executed, None if rejected/cancelled (e.g. FOK miss).
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if successfully cancelled."""
        ...

    @abstractmethod
    async def fetch_markets(self, category: Optional[str] = None) -> list[dict]:
        """
        List available markets, optionally filtered by category.
        Returns raw market metadata dicts.
        """
        ...

    @abstractmethod
    async def stream_order_book(
        self,
        market_ids: list[str],
        callback: Callable[[OrderBook], Awaitable[None]],
    ) -> None:
        """
        Subscribe to live order-book updates via WebSocket.
        Calls `callback` on every update.
        """
        ...
