"""Estimated API-equivalent cost of a provider run, from list prices per million tokens.

Subscription plans do not bill per token; the estimate still ranks lanes by how much of
the plan's capacity they consume. Cache reads are billed at a tenth of the input price
and cache writes at twice the input price (the one-hour cache Claude Code uses).
"""

from __future__ import annotations

from tasque2.providers.stream import TokenUsage

PRICES_PER_MTOK: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-sonnet-5": (2.0, 10.0),
    "claude-opus-4-8": (5.0, 25.0),
    "claude-opus-5-5": (4.0, 20.0),
    "claude-opus-5": (5.0, 25.0),
    "claude-fable-5-1": (10.0, 50.0),
    "claude-fable-5": (10.0, 50.0),
}
CACHE_READ_MULTIPLIER = 0.1
CACHE_WRITE_MULTIPLIER = 2.0


def list_price(model: str | None) -> tuple[float, float] | None:
    if not model:
        return None
    normalized = model.strip().lower()
    for prefix in sorted(PRICES_PER_MTOK, key=len, reverse=True):
        if normalized.startswith(prefix):
            return PRICES_PER_MTOK[prefix]
    return None


def estimate_cost_usd(model: str | None, usage: TokenUsage) -> float | None:
    price = list_price(model)
    if price is None:
        return None
    input_price, output_price = price
    return (
        usage.input_tokens * input_price
        + usage.cache_read_tokens * input_price * CACHE_READ_MULTIPLIER
        + usage.cache_write_tokens * input_price * CACHE_WRITE_MULTIPLIER
        + usage.output_tokens * output_price
    ) / 1_000_000
