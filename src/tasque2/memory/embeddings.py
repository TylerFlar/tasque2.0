"""Pluggable text embeddings for hybrid memory retrieval.

The default :class:`HashingEmbedder` hashes token unigrams and bigrams into a fixed-width
vector: offline, deterministic, and dependency-free apart from numpy. An
:class:`OpenAIEmbedder` is used when an API key is configured. Embedding is best effort:
callers fall back to keyword search when it fails. Every embedder returns L2-normalized
vectors, so cosine similarity is a dot product.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from functools import lru_cache
from typing import Protocol, runtime_checkable

import numpy as np

from tasque2.config import Settings, get_settings

_TOKEN_RE = re.compile(r"[a-z0-9]+")


@runtime_checkable
class Embedder(Protocol):
    @property
    def name(self) -> str:
        """Stable identifier stored next to each vector."""

    @property
    def dim(self) -> int:
        """Vector dimensionality."""

    def embed(self, texts: list[str]) -> list[list[float]]:
        """One L2-normalized vector per input text."""


def pack_vector(vector: list[float]) -> bytes:
    return np.asarray(vector, dtype=np.float32).tobytes()


def unpack_vector(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32)


def top_k_by_vector(
    query: list[float],
    candidates: list[tuple[str, bytes]],
    *,
    k: int,
) -> list[tuple[str, float]]:
    """The ``k`` highest-cosine ``(id, score)`` pairs among packed candidate vectors."""
    if not query or not candidates or k <= 0:
        return []
    query_vector = np.asarray(query, dtype=np.float32)
    usable = [(cid, unpack_vector(blob)) for cid, blob in candidates]
    usable = [(cid, vector) for cid, vector in usable if vector.shape == query_vector.shape]
    if not usable:
        return []
    matrix = np.vstack([vector for _, vector in usable])
    scores = matrix @ query_vector
    order = np.argsort(-scores)[:k]
    return [(usable[index][0], float(scores[index])) for index in order]


def _normalize(vector: np.ndarray) -> list[float]:
    norm = float(np.linalg.norm(vector))
    if norm <= 1e-12:
        return vector.astype(np.float32).tolist()
    return (vector / norm).astype(np.float32).tolist()


class HashingEmbedder:
    """Deterministic feature-hashing embedder over token unigrams and bigrams."""

    def __init__(self, dim: int = 256) -> None:
        self._dim = max(16, int(dim))

    @property
    def name(self) -> str:
        return f"hash-{self._dim}"

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            tokens = _TOKEN_RE.findall((text or "").lower())
            features = tokens + [f"{a}_{b}" for a, b in zip(tokens, tokens[1:], strict=False)]
            buckets = np.zeros(self._dim, dtype=np.float64)
            for feature in features:
                code = int.from_bytes(hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest(), "big")
                buckets[code % self._dim] += 1.0 if (code >> 63) & 1 else -1.0
            vectors.append(_normalize(buckets))
        return vectors


class OpenAIEmbedder:
    """Semantic embedder backed by the OpenAI embeddings API.

    Network and HTTP errors propagate so the caller can fall back to keyword search.
    """

    def __init__(self, *, api_key: str, model: str = "text-embedding-3-small", dim: int = 1536) -> None:
        self._api_key = api_key
        self._model = model
        self._dim = int(dim)

    @property
    def name(self) -> str:
        return f"openai:{self._model}"

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, texts: list[str]) -> list[list[float]]:
        import httpx

        response = httpx.post(
            "https://api.openai.com/v1/embeddings",
            content=json.dumps({"model": self._model, "input": [text or " " for text in texts]}),
            headers={"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()
        rows = sorted(response.json()["data"], key=lambda item: item["index"])
        return [_normalize(np.asarray(row["embedding"], dtype=np.float64)) for row in rows]


def _resolve_api_key(settings: Settings) -> str | None:
    key = (settings.openai_api_key or os.environ.get("OPENAI_API_KEY") or "").strip()
    return key or None


@lru_cache(maxsize=4)
def _build_embedder(provider: str, model: str, dim: int, api_key: str | None) -> Embedder | None:
    if provider == "none":
        return None
    if provider == "openai" or (provider == "auto" and api_key):
        if not api_key:
            return HashingEmbedder(dim=dim)
        return OpenAIEmbedder(api_key=api_key, model=model)
    return HashingEmbedder(dim=dim)


def get_embedder(settings: Settings | None = None) -> Embedder | None:
    """The configured embedder, or None when embeddings are disabled.

    ``embedding_provider``: ``auto`` (OpenAI when a key is present, else hashing),
    ``hash``, ``openai``, or ``none``.
    """
    settings = settings or get_settings()
    provider = (settings.embedding_provider or "auto").strip().lower()
    if provider in {"hash", "hashing"}:
        return HashingEmbedder(dim=settings.embedding_dim)
    return _build_embedder(provider, settings.embedding_model, int(settings.embedding_dim), _resolve_api_key(settings))


def reset_embedder_cache() -> None:
    _build_embedder.cache_clear()
