"""Async Kalshi trading bot: config, client, strategy, safety, loguru."""

import asyncio
import sys
from pathlib import Path

from loguru import logger

from client import KalshiClient, KalshiAPIError, MarketPrice
from config import TradingConfig
from safety import SafetyEngine
from strategy import MarketData, TradingStrategy


def setup_logging(log_dir: Path | None = None) -> None:
    """Configure loguru: stderr and optional file with rotation."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "kalshi_bot_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="7 days",
            level="INFO",
        )


def compute_exposure_dollars(positions: list[dict]) -> float:
    """Sum |position| * avg_price (in dollars). Prices from API are in cents."""
    total = 0.0
    for p in positions:
        pos = p.get("position", 0) or 0
        avg_price_cents = p.get("avg_price") or p.get("average_price") or 0
        total += abs(pos) * (float(avg_price_cents) / 100.0)
    return total


async def run_loop(
    config: TradingConfig,
    client: KalshiClient,
    strategy: TradingStrategy,
    safety: SafetyEngine,
) -> None:
    heartbeat_interval = 60
    loop_interval = 30
    default_order_count = 1

    while True:
        logger.info(
            "heartbeat",
            status="running",
            ticker_count=len(config.target_tickers),
        )
        try:
            positions = await client.get_positions()
            current_exposure = compute_exposure_dollars(positions)
        except KalshiAPIError as e:
            logger.warning("API error fetching positions", status_code=e.status_code, body=e.body)
            await asyncio.sleep(loop_interval)
            continue

        prices: list[MarketPrice | None] = await client.get_market_prices_batch(config.target_tickers)
        for i, ticker in enumerate(config.target_tickers):
            mp = prices[i] if i < len(prices) else None
            if mp is None:
                continue
            logger.info(
                "price_update",
                ticker=ticker,
                yes_bid_cents=mp.yes_bid_cents,
                yes_ask_cents=mp.yes_ask_cents,
                last_price_cents=mp.last_price_cents,
            )
            market_data = MarketData.from_market_price(mp)
            signal = strategy.get_signal(ticker, market_data)
            if signal == "Hold":
                continue
            side = "yes"
            action = "buy" if signal == "Buy" else "sell"
            price_cents = mp.mid_cents or mp.last_price_cents or 50
            if price_cents <= 0:
                continue
            count = default_order_count
            if not safety.can_trade(
                current_exposure,
                side,
                count,
                price_cents,
                config.max_exposure,
            ):
                continue
            try:
                resp = await client.place_limit_order(
                    ticker=ticker,
                    side=side,
                    action=action,
                    count=count,
                    yes_price_cents=price_cents,
                )
                order = resp.get("order", resp)
                order_id = order.get("order_id", "")
                client_order_id = order.get("client_order_id", "")
                status = order.get("status", "")
                logger.info(
                    "order_execution",
                    ticker=ticker,
                    side=side,
                    action=action,
                    count=count,
                    price_cents=price_cents,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    status=status,
                    success=True,
                )
                current_exposure += count * (price_cents / 100.0)
            except KalshiAPIError as e:
                logger.warning(
                    "order_execution failed",
                    ticker=ticker,
                    side=side,
                    count=count,
                    price_cents=price_cents,
                    status_code=e.status_code,
                    body=e.body,
                    success=False,
                )

        await asyncio.sleep(loop_interval)


async def main_async() -> None:
    setup_logging(Path("logs"))
    config = TradingConfig()
    private_key = config.load_private_key()
    client = KalshiClient(
        base_url=config.base_url,
        api_key_id=config.api_key_id,
        private_key=private_key,
    )
    strategy = TradingStrategy(threshold_cents=2.0)
    safety = SafetyEngine()
    async with client:
        await client.authenticate()
        await run_loop(config, client, strategy, safety)


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Shutting down")
    except KalshiAPIError as e:
        logger.error("Kalshi API error", status_code=e.status_code, body=e.body)
        sys.exit(1)
    except Exception as e:
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
