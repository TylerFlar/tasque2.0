from __future__ import annotations

from tasque2.embeddings import (
    HashingEmbedder,
    cosine,
    pack_vector,
    top_k_by_vector,
    unpack_vector,
)
from tasque2.retrieval import select_relevant_excerpt


def test_hashing_embedder_is_deterministic_and_unit_normalized() -> None:
    embedder = HashingEmbedder(dim=128)
    first = embedder.embed(["beginner cooking class"])[0]
    second = embedder.embed(["beginner cooking class"])[0]
    assert first == second
    assert embedder.name == "hash-128"
    assert embedder.dim == 128
    assert abs(sum(value * value for value in first) - 1.0) < 1e-6


def test_hashing_embedder_related_beats_unrelated() -> None:
    embedder = HashingEmbedder(dim=256)
    cooking, cooking_two, archery = embedder.embed(
        ["cooking class pasta", "cooking workshop pasta night", "archery bow shooting range"]
    )
    assert cosine(cooking, cooking_two) > cosine(cooking, archery)


def test_pack_unpack_roundtrip() -> None:
    vector = [0.1, -0.2, 0.3, 0.0]
    restored = unpack_vector(pack_vector(vector))
    assert len(restored) == len(vector)
    assert all(abs(a - b) < 1e-6 for a, b in zip(vector, restored, strict=True))


def test_top_k_by_vector_orders_by_cosine() -> None:
    query = [1.0, 0.0]
    candidates = [("a", [1.0, 0.0]), ("b", [0.0, 1.0]), ("c", [0.7, 0.7])]
    ranked = top_k_by_vector(query, candidates, k=2)
    assert [cid for cid, _ in ranked] == ["a", "c"]


def test_select_excerpt_returns_full_when_under_budget() -> None:
    doc = "# Heading\nshort body"
    excerpt, trimmed = select_relevant_excerpt(doc, "anything", budget_chars=1000)
    assert excerpt == doc
    assert trimmed is False


def test_select_excerpt_keeps_relevant_middle_and_drops_filler() -> None:
    lede = "# Profile\nGeneral framing of the person here."
    filler_a = "## Weather\n" + ("rain " * 60)
    target = "## Cooking\nHe loves beginner cooking classes and pasta making workshops."
    filler_b = "## Geology\n" + ("rocks " * 60)
    doc = "\n\n".join([lede, filler_a, target, filler_b])

    excerpt, trimmed = select_relevant_excerpt(
        doc,
        "cooking classes workshops",
        budget_chars=len(lede) + len(target) + 90,
        position_bias=0.0,
    )
    assert trimmed is True
    assert "beginner cooking classes" in excerpt  # the relevant middle survived
    assert "Geology" not in excerpt  # irrelevant filler dropped
