"""Async Kalshi API client using httpx with request-level RSA-PSS signing."""

import asyncio
import base64
import time
import uuid
from typing import Any

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes

from loguru import logger


class KalshiAPIError(Exception):
    """Raised on HTTP 4xx/5xx from Kalshi API."""

    def __init__(self, status_code: int, body: str | bytes):
        self.status_code = status_code
        self.body = body if isinstance(body, str) else body.decode("utf-8", errors="replace")
        super().__init__(f"Kalshi API Error {status_code}: {self.body}")


def _sign_request(private_key: PrivateKeyTypes, timestamp: str, method: str, path: str) -> str:
    """Create RSA-PSS SHA256 signature. Path must not include query string."""
    path_without_query = path.split("?")[0]
    message = f"{timestamp}{method.upper()}{path_without_query}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def _auth_headers(
    private_key: PrivateKeyTypes,
    api_key_id: str,
    method: str,
    path: str,
) -> dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    signature = _sign_request(private_key, timestamp, method, path)
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": signature,
    }


class MarketPrice:
    """Simple market price data for strategy consumption."""

    def __init__(
        self,
        ticker: str,
        yes_bid_cents: int | None,
        yes_ask_cents: int | None,
        last_price_cents: int | None = None,
    ):
        self.ticker = ticker
        self.yes_bid_cents = yes_bid_cents
        self.yes_ask_cents = yes_ask_cents
        self.last_price_cents = last_price_cents or yes_bid_cents or yes_ask_cents or 0

    @property
    def yes_bid(self) -> float:
        return (self.yes_bid_cents or 0) / 100.0

    @property
    def yes_ask(self) -> float:
        return (self.yes_ask_cents or 0) / 100.0

    @property
    def mid_cents(self) -> int:
        bid = self.yes_bid_cents or 0
        ask = self.yes_ask_cents or 0
        if bid and ask:
            return (bid + ask) // 2
        return bid or ask or self.last_price_cents or 0


class KalshiClient:
    """Async Kalshi API client with authenticate, market prices, and limit orders."""

    BASE_PATH = "/trade-api/v2"

    def __init__(
        self,
        base_url: str,
        api_key_id: str,
        private_key: PrivateKeyTypes,
        timeout: float = 30.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._api_key_id = api_key_id
        self._private_key = private_key
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
            )
        return self._client

    def _headers(self, method: str, path: str) -> dict[str, str]:
        return _auth_headers(self._private_key, self._api_key_id, method, path)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        full_path = self.BASE_PATH + path
        headers = self._headers(method, full_path)
        headers["Content-Type"] = "application/json"
        client = await self._get_client()
        max_retries = 3
        last_error: KalshiAPIError | None = None
        for attempt in range(max_retries):
            try:
                resp = await client.request(
                    method,
                    full_path,
                    headers=headers,
                    params=params,
                    json=json,
                )
                if resp.status_code >= 400:
                    exc = KalshiAPIError(resp.status_code, resp.content)
                    if resp.status_code == 429 or resp.status_code >= 500:
                        last_error = exc
                        if attempt < max_retries - 1:
                            delay = 2**attempt
                            logger.warning("API error, retrying", attempt=attempt + 1, delay=delay, status_code=resp.status_code)
                            await asyncio.sleep(delay)
                            headers = self._headers(method, full_path)
                            headers["Content-Type"] = "application/json"
                            continue
                    raise exc
                if resp.status_code == 204 or not resp.content:
                    return {}
                return resp.json()
            except KalshiAPIError:
                raise
        if last_error:
            raise last_error
        raise KalshiAPIError(0, "unknown")

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "KalshiClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def authenticate(self) -> None:
        """Verify credentials by fetching balance. Raises on 401."""
        try:
            data = await self._request("GET", "/portfolio/balance")
            balance_cents = data.get("balance", 0)
            logger.info("Authentication successful", balance_cents=balance_cents)
        except KalshiAPIError as e:
            if e.status_code == 401:
                raise KalshiAPIError(e.status_code, e.body) from e
            raise

    async def get_balance(self) -> int:
        """Return balance in cents."""
        data = await self._request("GET", "/portfolio/balance")
        return int(data.get("balance", 0))

    async def get_positions(self) -> list[dict[str, Any]]:
        """Return list of position objects (ticker, position, avg_price, etc.)."""
        data = await self._request("GET", "/portfolio/positions", params={"limit": 200})
        return data.get("market_positions", data.get("positions", []))

    async def get_market(self, ticker: str) -> dict[str, Any]:
        """Get single market by ticker."""
        return await self._request("GET", f"/markets/{ticker}")

    async def get_orderbook(self, ticker: str) -> dict[str, Any]:
        """Get orderbook for a market."""
        return await self._request("GET", f"/markets/{ticker}/orderbook")

    async def get_market_prices(self, ticker: str) -> MarketPrice | None:
        """Fetch market + orderbook for one ticker; return MarketPrice or None on error."""
        try:
            market_task = self.get_market(ticker)
            orderbook_task = self.get_orderbook(ticker)
            market_resp, ob_resp = await market_task, await orderbook_task
        except KalshiAPIError:
            return None
        m = market_resp.get("market") or market_resp
        yes_bid = m.get("yes_bid")
        yes_ask = m.get("yes_ask")
        last = m.get("last_price") or m.get("yes_price")
        if yes_bid is None and ob_resp:
            ob = ob_resp.get("orderbook", {})
            yes_levels = ob.get("yes", [])
            no_levels = ob.get("no", [])
            if yes_levels:
                yes_bid = yes_levels[0][0] if isinstance(yes_levels[0], (list, tuple)) else yes_levels[0]
            if no_levels:
                no_best = no_levels[0][0] if isinstance(no_levels[0], (list, tuple)) else no_levels[0]
                yes_ask = 100 - no_best if no_best is not None else yes_ask
        yes_bid_cents = int(yes_bid) if yes_bid is not None else None
        yes_ask_cents = int(yes_ask) if yes_ask is not None else None
        last_cents = int(last) if last is not None else None
        return MarketPrice(
            ticker=ticker,
            yes_bid_cents=yes_bid_cents,
            yes_ask_cents=yes_ask_cents,
            last_price_cents=last_cents,
        )

    async def get_market_prices_batch(self, tickers: list[str]) -> list[MarketPrice | None]:
        """Fetch market prices for multiple tickers concurrently."""
        import asyncio
        results = await asyncio.gather(
            *[self.get_market_prices(t) for t in tickers],
            return_exceptions=True,
        )
        out: list[MarketPrice | None] = []
        for r in results:
            if isinstance(r, Exception):
                logger.warning("Failed to fetch price", error=str(r))
                out.append(None)
            else:
                out.append(r)
        return out

    async def place_limit_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        yes_price_cents: int,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Place a limit order. side: 'yes'|'no', action: 'buy'|'sell'. Returns order response."""
        payload = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": count,
            "type": "limit",
            "yes_price": yes_price_cents,
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        data = await self._request("POST", "/portfolio/orders", json=payload)
        return data
