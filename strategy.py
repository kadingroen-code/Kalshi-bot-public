"""Trading strategy: Fair Value vs Market Price -> Buy/Sell/Hold."""

from dataclasses import dataclass
from typing import Literal

Signal = Literal["Buy", "Sell", "Hold"]


@dataclass
class MarketData:
    """Market data for one ticker (prices in dollars for strategy logic)."""

    ticker: str
    yes_bid: float
    yes_ask: float
    mid: float

    @classmethod
    def from_market_price(cls, mp: "MarketPrice") -> "MarketData":
        """Build from client's MarketPrice."""
        bid = mp.yes_bid
        ask = mp.yes_ask
        mid = (bid + ask) / 2.0 if (bid and ask) else (bid or ask or 0.0)
        return cls(ticker=mp.ticker, yes_bid=bid, yes_ask=ask, mid=mid)


class TradingStrategy:
    """
    Fair Value vs Market Price comparison.
    Fair value = mid from market data. Market price = mid.
    Buy if market_price < fair_value - threshold, Sell if market_price > fair_value + threshold, else Hold.
    """

    def __init__(self, threshold_cents: float = 2.0):
        self.threshold_cents = threshold_cents
        self.threshold_dollars = threshold_cents / 100.0

    def get_signal(self, ticker: str, market_data: MarketData) -> Signal:
        fair_value = market_data.mid
        market_price = market_data.mid
        if market_price < fair_value - self.threshold_dollars:
            return "Buy"
        if market_price > fair_value + self.threshold_dollars:
            return "Sell"
        return "Hold"
