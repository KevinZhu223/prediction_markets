"""
Shared data models used across the entire bot.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time
import uuid


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class ContractSide(str, Enum):
    YES = "yes"
    NO = "no"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"
    FOK = "fok"  # Fill-or-Kill


class Platform(str, Enum):
    KALSHI = "kalshi"
    KALSHI_STANDARD = "kalshi_standard"


@dataclass
class OrderBookLevel:
    price: float  # 0.00 - 1.00 (probability)
    quantity: int


@dataclass
class OrderBook:
    market_id: str
    platform: Platform
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None


@dataclass
class Order:
    market_id: str
    platform: Platform
    side: Side  # buy/sell
    contract_side: ContractSide
    price: float
    quantity: int
    order_type: OrderType = OrderType.FOK
    client_order_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class Fill:
    order: Order
    fill_price: float
    fill_quantity: int
    fees: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class Position:
    market_id: str
    platform: Platform
    side: Side
    contract_side: ContractSide
    quantity: int
    avg_price: float
    unrealized_pnl: float = 0.0


@dataclass
class SpotPriceUpdate:
    symbol: str  # e.g. "BTCUSDT"
    price: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class Signal:
    """A trading signal emitted by a strategy."""
    strategy: str
    action: Side
    contract_side: ContractSide
    platform: Platform
    market_id: str
    target_price: float
    quantity: int
    confidence: float  # 0.0 - 1.0
    reason: str
    timestamp: float = field(default_factory=time.time)
