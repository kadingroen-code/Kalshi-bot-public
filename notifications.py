"""Optional Discord and Supabase notifications for the async bot."""

import asyncio
from typing import Any, Optional

import httpx
from loguru import logger


async def send_discord_async(webhook_url: str, message: str, username: str = "Kalshi Trading Bot") -> bool:
    """Send a message to a Discord webhook. Returns True on success."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                webhook_url,
                json={"content": message, "username": username},
            )
            if resp.status_code == 204:
                logger.debug("Discord notification sent")
                return True
            logger.warning("Discord webhook failed", status_code=resp.status_code, body=resp.text)
            return False
    except Exception as e:
        logger.warning("Discord send error", error=str(e))
        return False


def update_supabase_position_sync(supabase_url: str, supabase_key: str, position_id: int, status: str, quantity: int) -> bool:
    """Update a position row in Supabase (sync). Returns True on success."""
    try:
        from supabase import create_client
        client = create_client(supabase_url, supabase_key)
        client.table("positions").update({"status": status, "quantity": quantity}).eq("id", position_id).execute()
        logger.debug("Supabase position updated", position_id=position_id)
        return True
    except Exception as e:
        logger.warning("Supabase update error", position_id=position_id, error=str(e))
        return False


async def update_supabase_position(
    supabase_url: str, supabase_key: str, position_id: Optional[int], status: str, quantity: int
) -> bool:
    """Update position in Supabase. If position_id is None, no-op and return True."""
    if position_id is None:
        return True
    return await asyncio.to_thread(update_supabase_position_sync, supabase_url, supabase_key, position_id, status, quantity)
