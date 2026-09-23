"""Durable memory: canonical documents, atomic facts, hybrid recall, and text ingestion."""

from tasque2.memory.service import (
    CANONICAL_BUDGET_RE,
    MemoryBudgetExceeded,
    MemoryService,
    ScoredMemory,
    canonical_budget,
    expire_ttl_memories,
    safe_fts_query,
)

__all__ = [
    "CANONICAL_BUDGET_RE",
    "MemoryBudgetExceeded",
    "MemoryService",
    "ScoredMemory",
    "canonical_budget",
    "expire_ttl_memories",
    "safe_fts_query",
]
