"""Trading strategies: Fair Value vs Market Price, and Risk-Neutralization (hedge at 50%)."""

from dataclasses import dataclass
from math import floor
from typing import Any, Literal

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


class RiskNeutralizationStrategy:
    """
    Hedge existing positions when price is up 50%: sell enough contracts to recover initial capital.
    get_hedge_signal returns (contracts_to_sell, price_cents) or None if no hedge.
    """

    def __init__(self, gain_threshold: float = 0.50, min_investment_to_hedge: float = 10.0):
        self.gain_threshold = gain_threshold
        self.min_investment_to_hedge = min_investment_to_hedge

    def get_hedge_signal(
        self,
        position: dict[str, Any],
        current_price_dollars: float,
    ) -> tuple[int, int] | None:
        """
        If position has gained >= gain_threshold, return (contracts_to_sell, price_cents).
        Otherwise return None. Position dict: position (qty), avg_price or average_price (cents).
        """
        pos = position.get("position", 0) or 0
        avg_cents = position.get("avg_price") or position.get("average_price") or 0
        entry_price_dollars = float(avg_cents) / 100.0
        invested_dollars = abs(pos) * entry_price_dollars
        if invested_dollars < self.min_investment_to_hedge or current_price_dollars <= 0:
            return None
        percent_gain = (current_price_dollars - entry_price_dollars) / entry_price_dollars
        if percent_gain < self.gain_threshold:
            return None
        initial_capital = entry_price_dollars * pos
        contracts_to_sell = floor(initial_capital / current_price_dollars)
        contracts_to_sell = max(0, min(contracts_to_sell, pos - 1))
        if contracts_to_sell <= 0:
            return None
        price_cents = int(round(current_price_dollars * 100))
        return (contracts_to_sell, price_cents)
