"""Pluggable text embeddings for hybrid memory retrieval.

Design goals for this single-user, local daemon:

- **No heavy dependencies.** The default :class:`HashingEmbedder` is pure
  stdlib (feature hashing), so embeddings work offline, deterministically, and
  in tests with zero setup. A real semantic model (:class:`OpenAIEmbedder`) is
  used only when an API key is configured.
- **Never break a write.** Embedding is best-effort: callers wrap embed calls so
  a provider/network failure degrades retrieval to keyword (FTS) search rather
  than failing the memory write.
- **Cosine == dot product.** Every embedder returns L2-normalized vectors, so
  similarity is a plain dot product and stored norms are unnecessary.

Vectors are stored as packed ``float32`` bytes (see :func:`pack_vector`); at the
data volume of one user (a few thousand memories) a brute-force dot product in
Python is sub-millisecond per query, so no vector database is required. If
``numpy`` happens to be importable it is used to accelerate scoring, but it is
not a dependency.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from array import array
from functools import lru_cache
from typing import Protocol, runtime_checkable

from tasque2.config import Settings, get_settings

_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Optional acceleration; absent by default (numpy is not a project dependency).
try:  # pragma: no cover - exercised only when numpy is installed
    import numpy as _np
except Exception:  # pragma: no cover
    _np = None


@runtime_checkable
class Embedder(Protocol):
    """A text -> unit-vector encoder."""

    @property
    def name(self) -> str:
        """Stable identifier stored alongside vectors (e.g. ``hash-256``)."""

    @property
    def dim(self) -> int:
        """Vector dimensionality."""

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one L2-normalized vector per input text."""


# --------------------------------------------------------------------------- #
# Vector helpers
# --------------------------------------------------------------------------- #
def pack_vector(vector: list[float]) -> bytes:
    """Pack a float vector into compact ``float32`` bytes for storage."""
    return array("f", vector).tobytes()


def unpack_vector(blob: bytes) -> list[float]:
    """Inverse of :func:`pack_vector`."""
    out = array("f")
    out.frombytes(blob)
    return list(out)


def _normalize(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm <= 1e-12:
        return vector
    inverse = 1.0 / norm
    return [value * inverse for value in vector]


def cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity of two same-length vectors (== dot for unit vectors)."""
    if not a or not b or len(a) != len(b):
        return 0.0
    return sum(x * y for x, y in zip(a, b, strict=False))


def top_k_by_vector(
    query: list[float],
    candidates: list[tuple[str, list[float]]],
    *,
    k: int,
) -> list[tuple[str, float]]:
    """Return the ``k`` highest-cosine ``(id, score)`` pairs.

    Uses numpy when available, else a pure-Python dot product. Inputs are assumed
    L2-normalized (the embedders guarantee this), so cosine == dot product.
    """
    if not query or not candidates or k <= 0:
        return []
    if _np is not None:  # pragma: no cover - only when numpy present
        ids = [cid for cid, _ in candidates]
        matrix = _np.asarray([vec for _, vec in candidates], dtype=_np.float32)
        scores = matrix @ _np.asarray(query, dtype=_np.float32)
        order = _np.argsort(-scores)[:k]
        return [(ids[i], float(scores[i])) for i in order]
    scored = [(cid, cosine(query, vec)) for cid, vec in candidates]
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored[:k]


# --------------------------------------------------------------------------- #
# Embedders
# --------------------------------------------------------------------------- #
def _tokens(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


class HashingEmbedder:
    """Deterministic, dependency-free feature-hashing embedder.

    Hashes token unigrams and bigrams into a fixed-width signed bag-of-words and
    L2-normalizes. Cosine similarity then approximates weighted lexical overlap.
    It is not semantic, but it is a valid, offline, reproducible vector channel —
    the safe default and the baseline used by tests.
    """

    def __init__(self, dim: int = 256) -> None:
        self._dim = max(16, int(dim))

    @property
    def name(self) -> str:
        return f"hash-{self._dim}"

    @property
    def dim(self) -> int:
        return self._dim

    def _features(self, text: str) -> list[str]:
        tokens = _tokens(text)
        bigrams = [f"{a}_{b}" for a, b in zip(tokens, tokens[1:], strict=False)]
        return tokens + bigrams

    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            buckets = [0.0] * self._dim
            for feature in self._features(text or ""):
                digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
                code = int.from_bytes(digest, "big")
                index = code % self._dim
                sign = 1.0 if (code >> 63) & 1 else -1.0
                buckets[index] += sign
            vectors.append(_normalize(buckets))
        return vectors


class OpenAIEmbedder:
    """Semantic embedder backed by the OpenAI embeddings API (stdlib HTTP).

    Used only when an API key is configured. Network/HTTP errors propagate so the
    caller can fall back to keyword search; they must not be swallowed here.
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
        import urllib.request

        payload = json.dumps({"model": self._model, "input": [t or " " for t in texts]}).encode("utf-8")
        request = urllib.request.Request(
            "https://api.openai.com/v1/embeddings",
            data=payload,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - fixed trusted URL
            body = json.loads(response.read().decode("utf-8"))
        rows = sorted(body["data"], key=lambda item: item["index"])
        return [_normalize([float(value) for value in row["embedding"]]) for row in rows]


def _resolve_api_key(settings: Settings) -> str | None:
    key = (settings.openai_api_key or os.environ.get("OPENAI_API_KEY") or "").strip()
    return key or None


@lru_cache(maxsize=4)
def _build_embedder(provider: str, model: str, dim: int, api_key: str | None) -> Embedder | None:
    if provider == "none":
        return None
    if provider in {"openai"} or (provider == "auto" and api_key):
        if not api_key:
            # 'openai' explicitly requested but no key -> degrade rather than crash.
            return HashingEmbedder(dim=dim)
        return OpenAIEmbedder(api_key=api_key, model=model)
    return HashingEmbedder(dim=dim)


def get_embedder(settings: Settings | None = None) -> Embedder | None:
    """Return the configured embedder, or ``None`` when embeddings are disabled.

    ``embedding_provider``: ``auto`` (OpenAI if a key is present, else hashing),
    ``hash``, ``openai``, or ``none``.
    """
    settings = settings or get_settings()
    provider = (settings.embedding_provider or "auto").strip().lower()
    if provider == "hash":
        provider = "hashing"
    if provider == "hashing":
        return HashingEmbedder(dim=settings.embedding_dim)
    return _build_embedder(
        provider,
        settings.embedding_model,
        int(settings.embedding_dim),
        _resolve_api_key(settings),
    )


def reset_embedder_cache() -> None:
    _build_embedder.cache_clear()
