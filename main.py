"""Async Kalshi trading bot: config, client, strategy, safety, loguru. Single entrypoint."""

import asyncio
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from client import KalshiClient, KalshiAPIError, MarketPrice
from config import TradingConfig
from notifications import send_discord_async, update_supabase_position
from safety import SafetyEngine
from strategy import MarketData, RiskNeutralizationStrategy, TradingStrategy


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


def _position_ticker(p: dict) -> str:
    return p.get("ticker") or p.get("market_ticker") or ""


async def run_loop_fair_value(
    config: TradingConfig,
    client: KalshiClient,
    strategy: TradingStrategy,
    safety: SafetyEngine,
    discord_url: Optional[str] = None,
) -> None:
    """Fair Value vs Market Price loop: target_tickers, signals Buy/Sell/Hold."""
    loop_interval = config.loop_interval_seconds
    order_size = config.order_size
    while True:
        logger.info(
            "heartbeat",
            status="running",
            strategy="fair_value",
            ticker_count=len(config.target_tickers),
        )
        try:
            positions = await client.get_positions()
            current_exposure = compute_exposure_dollars(positions)
        except KalshiAPIError as e:
            logger.warning("API error fetching positions", status_code=e.status_code, body=e.body)
            if discord_url:
                await send_discord_async(discord_url, f"API error fetching positions: {e.status_code}")
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
            count = order_size
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
                if discord_url:
                    await send_discord_async(discord_url, f"Order {action} {count} {ticker} @ {price_cents}c | order_id={order_id}")
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
                if discord_url:
                    await send_discord_async(discord_url, f"Order execution failed: {ticker} - {e.status_code} {e.body}")

        await asyncio.sleep(loop_interval)


async def run_loop_risk_neutral(
    config: TradingConfig,
    client: KalshiClient,
    strategy: RiskNeutralizationStrategy,
    safety: SafetyEngine,
    discord_url: Optional[str] = None,
    supabase_url: Optional[str] = None,
    supabase_key: Optional[str] = None,
) -> None:
    """Risk-neutralization loop: hedge positions when up 50%."""
    loop_interval = config.loop_interval_seconds
    min_investment = config.min_investment_to_hedge
    while True:
        logger.info(
            "heartbeat",
            status="running",
            strategy="risk_neutral",
        )
        try:
            positions = await client.get_positions()
            current_exposure = compute_exposure_dollars(positions)
        except KalshiAPIError as e:
            logger.warning("API error fetching positions", status_code=e.status_code, body=e.body)
            if discord_url:
                await send_discord_async(discord_url, f"API error fetching positions: {e.status_code}")
            await asyncio.sleep(loop_interval)
            continue

        for p in positions:
            ticker = _position_ticker(p)
            if not ticker:
                continue
            pos = p.get("position", 0) or 0
            avg_cents = p.get("avg_price") or p.get("average_price") or 0
            invested = abs(pos) * (float(avg_cents) / 100.0)
            if invested < min_investment:
                continue
            try:
                mp = await client.get_market_prices(ticker)
            except KalshiAPIError:
                continue
            if mp is None:
                continue
            current_price_dollars = mp.yes_bid or (mp.last_price_cents / 100.0 if mp.last_price_cents else 0)
            if current_price_dollars <= 0:
                continue
            logger.info(
                "price_update",
                ticker=ticker,
                yes_bid_cents=mp.yes_bid_cents,
                last_price_cents=mp.last_price_cents,
            )
            hedge = strategy.get_hedge_signal(p, current_price_dollars)
            if hedge is None:
                continue
            contracts_to_sell, price_cents = hedge
            if not safety.can_trade(
                current_exposure,
                "yes",
                contracts_to_sell,
                price_cents,
                config.max_exposure,
            ):
                continue
            position_id = p.get("id")
            try:
                resp = await client.place_limit_order(
                    ticker=ticker,
                    side="yes",
                    action="sell",
                    count=contracts_to_sell,
                    yes_price_cents=price_cents,
                )
                order = resp.get("order", resp)
                order_id = order.get("order_id", "")
                logger.info(
                    "order_execution",
                    ticker=ticker,
                    side="yes",
                    action="sell",
                    count=contracts_to_sell,
                    price_cents=price_cents,
                    order_id=order_id,
                    success=True,
                )
                current_exposure += contracts_to_sell * (price_cents / 100.0)
                remaining = pos - contracts_to_sell
                if supabase_url and supabase_key:
                    await update_supabase_position(supabase_url, supabase_key, position_id, "HEDGED", remaining)
                if discord_url:
                    percent_gain = (current_price_dollars - (float(avg_cents) / 100.0)) / (float(avg_cents) / 100.0) * 100
                    await send_discord_async(
                        discord_url,
                        f"HEDGE EXECUTED: {ticker} | Sold {contracts_to_sell} @ {price_cents}c | "
                        f"Remaining {remaining} | Gain {percent_gain:.1f}%",
                    )
            except KalshiAPIError as e:
                logger.warning(
                    "order_execution failed",
                    ticker=ticker,
                    count=contracts_to_sell,
                    price_cents=price_cents,
                    status_code=e.status_code,
                    body=e.body,
                    success=False,
                )
                if discord_url:
                    await send_discord_async(discord_url, f"Order execution error: {ticker} - {e.status_code} {e.body}")

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
    safety = SafetyEngine()
    discord_url = config.discord_webhook_url
    supabase_url = config.supabase_url
    supabase_key = config.supabase_key

    async with client:
        await client.authenticate()
        if config.strategy == "fair_value":
            strategy = TradingStrategy(threshold_cents=config.threshold_cents)
            await run_loop_fair_value(config, client, strategy, safety, discord_url)
        else:
            strategy = RiskNeutralizationStrategy(
                gain_threshold=0.50,
                min_investment_to_hedge=config.min_investment_to_hedge,
            )
            await run_loop_risk_neutral(
                config,
                client,
                strategy,
                safety,
                discord_url=discord_url,
                supabase_url=supabase_url,
                supabase_key=supabase_key,
            )


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
