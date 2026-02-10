"""Safety engine: enforce max_exposure before sending orders."""

from loguru import logger


class SafetyEngine:
    """
    Check that a proposed trade would not push total exposure above max_exposure.
    Exposure = sum of |position| * price per position, plus proposed notional.
    """

    def can_trade(
        self,
        current_exposure_dollars: float,
        side: str,
        count: int,
        price_cents: int,
        max_exposure: float,
    ) -> bool:
        """
        Return True if current_exposure + notional of proposed trade <= max_exposure.
        side: 'yes' or 'no' (for logging); notional = count * (price_cents/100).
        """
        notional = count * (price_cents / 100.0)
        new_exposure = current_exposure_dollars + notional
        allowed = new_exposure <= max_exposure
        if allowed:
            logger.info(
                "SafetyEngine allow",
                current_exposure=current_exposure_dollars,
                proposed_notional=notional,
                new_exposure=new_exposure,
                max_exposure=max_exposure,
                side=side,
                count=count,
                price_cents=price_cents,
            )
        else:
            logger.warning(
                "SafetyEngine reject: would exceed max_exposure",
                current_exposure=current_exposure_dollars,
                proposed_notional=notional,
                new_exposure=new_exposure,
                max_exposure=max_exposure,
                side=side,
                count=count,
                price_cents=price_cents,
            )
        return allowed
