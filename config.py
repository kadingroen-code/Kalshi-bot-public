"""Pydantic config schema for the Kalshi trading bot."""

from pathlib import Path
from typing import Literal, Optional

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

StrategyName = Literal["fair_value", "risk_neutral"]


class TradingConfig(BaseSettings):
    """Configuration loaded from environment and/or .env file."""

    model_config = SettingsConfigDict(
        env_prefix="KALSHI_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_key_id: str
    base_url: str = "https://demo-api.kalshi.co"
    max_exposure: float
    target_tickers: list[str] = Field(default_factory=list, validation_alias="TICKERS")

    # Strategy: "fair_value" (default) or "risk_neutral" (hedge at 50% gain)
    strategy: StrategyName = "fair_value"
    # Tuning
    heartbeat_interval_seconds: int = 60
    loop_interval_seconds: int = 30
    order_size: int = 1
    threshold_cents: float = 2.0
    # Risk-neutral only: min position value ($) to consider for hedging
    min_investment_to_hedge: float = 10.0

    # Optional: Discord webhook URL (env: KALSHI_DISCORD_WEBHOOK_URL)
    discord_webhook_url: Optional[str] = None
    # Optional: Supabase URL and key (env: KALSHI_SUPABASE_URL, KALSHI_SUPABASE_KEY)
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None

    # Private key: provide either path to .key file or PEM string (e.g. env KALSHI_PRIVATE_KEY_PEM)
    private_key_path: Optional[str] = None
    private_key_pem: Optional[str] = None

    @model_validator(mode="after")
    def require_private_key_source(self):
        if not self.private_key_path and not self.private_key_pem:
            raise ValueError("Must set either KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PEM")
        if self.private_key_path and self.private_key_pem:
            raise ValueError("Set only one of KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PEM")
        if self.strategy == "fair_value" and not self.target_tickers:
            raise ValueError("KALSHI_TICKERS required when strategy is fair_value")
        return self

    @field_validator("target_tickers", mode="before")
    @classmethod
    def parse_tickers(cls, v):
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return v

    def load_private_key(self):
        """Load RSA private key from path or PEM string. Call once at startup."""
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        if self.private_key_pem:
            return serialization.load_pem_private_key(
                self.private_key_pem.encode("utf-8"),
                password=None,
                backend=default_backend(),
            )
        path = Path(self.private_key_path)
        if not path.exists():
            raise FileNotFoundError(f"Private key file not found: {path}")
        with path.open("rb") as f:
            return serialization.load_pem_private_key(
                f.read(),
                password=None,
                backend=default_backend(),
            )
