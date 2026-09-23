from __future__ import annotations

import pytest

from tasque2.config import reset_settings
from tasque2.memory.embeddings import (
    HashingEmbedder,
    OpenAIEmbedder,
    get_embedder,
    pack_vector,
    top_k_by_vector,
    unpack_vector,
)
from tasque2.memory.excerpt import select_relevant_excerpt


def test_hashing_embedder_is_deterministic_and_unit_normalized() -> None:
    embedder = HashingEmbedder(dim=128)
    first = embedder.embed(["beginner cooking class"])[0]
    second = embedder.embed(["beginner cooking class"])[0]
    assert first == second
    assert embedder.name == "hash-128"
    assert embedder.dim == 128
    assert abs(sum(value * value for value in first) - 1.0) < 1e-6


def test_hashing_embedder_ranks_related_text_above_unrelated() -> None:
    embedder = HashingEmbedder(dim=256)
    cooking, cooking_two, archery = embedder.embed(
        ["cooking class pasta", "cooking workshop pasta night", "archery bow shooting range"]
    )
    ranked = top_k_by_vector(cooking, [("archery", pack_vector(archery)), ("cooking", pack_vector(cooking_two))], k=2)
    assert [candidate for candidate, _ in ranked] == ["cooking", "archery"]
    assert ranked[0][1] > ranked[1][1]


def test_hashing_embedder_keeps_a_minimum_dimension() -> None:
    assert HashingEmbedder(dim=4).dim == 16


def test_empty_text_embeds_to_a_zero_vector() -> None:
    assert HashingEmbedder(dim=16).embed([""])[0] == [0.0] * 16


def test_pack_unpack_roundtrip() -> None:
    vector = [0.1, -0.2, 0.3, 0.0]
    restored = unpack_vector(pack_vector(vector))
    assert len(restored) == len(vector)
    assert all(abs(a - b) < 1e-6 for a, b in zip(vector, restored, strict=True))


def test_top_k_by_vector_orders_by_cosine() -> None:
    candidates = [
        ("a", pack_vector([1.0, 0.0])),
        ("b", pack_vector([0.0, 1.0])),
        ("c", pack_vector([0.7, 0.7])),
    ]
    ranked = top_k_by_vector([1.0, 0.0], candidates, k=2)
    assert [candidate for candidate, _ in ranked] == ["a", "c"]


def test_top_k_by_vector_skips_vectors_of_another_dimension() -> None:
    candidates = [("wide", pack_vector([1.0, 0.0, 0.0])), ("match", pack_vector([0.0, 1.0]))]
    assert [candidate for candidate, _ in top_k_by_vector([1.0, 0.0], candidates, k=5)] == ["match"]
    assert top_k_by_vector([1.0, 0.0], [("wide", pack_vector([1.0, 0.0, 0.0]))], k=5) == []
    assert top_k_by_vector([], candidates, k=5) == []
    assert top_k_by_vector([1.0, 0.0], candidates, k=0) == []


def test_get_embedder_hashes_offline_without_an_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("TASQUE2_EMBEDDING_DIM", "64")
    reset_settings()
    embedder = get_embedder()
    assert isinstance(embedder, HashingEmbedder)
    assert embedder.name == "hash-64"


@pytest.mark.parametrize(("provider", "expected"), [("hash", HashingEmbedder), ("openai", OpenAIEmbedder)])
def test_get_embedder_honors_the_configured_provider(
    monkeypatch: pytest.MonkeyPatch, provider: str, expected: type
) -> None:
    monkeypatch.setenv("TASQUE2_EMBEDDING_PROVIDER", provider)
    monkeypatch.setenv("TASQUE2_OPENAI_API_KEY", "sk-test")
    reset_settings()
    assert isinstance(get_embedder(), expected)


def test_get_embedder_uses_openai_when_a_key_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("TASQUE2_EMBEDDING_MODEL", "text-embedding-3-large")
    reset_settings()
    embedder = get_embedder()
    assert isinstance(embedder, OpenAIEmbedder)
    assert embedder.name == "openai:text-embedding-3-large"


def test_get_embedder_returns_none_when_embeddings_are_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASQUE2_EMBEDDING_PROVIDER", "none")
    reset_settings()
    assert get_embedder() is None


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
    assert excerpt.startswith("# Profile")
    assert "beginner cooking classes" in excerpt
    assert "Geology" not in excerpt
    assert "omitted" in excerpt


def test_select_excerpt_position_bias_prefers_later_sections() -> None:
    sections = [f"## Entry {index}\n" + ("note " * 40) for index in range(6)]
    doc = "\n\n".join(sections)
    excerpt, trimmed = select_relevant_excerpt(doc, "", budget_chars=len(sections[0]) * 2 + 120, position_bias=1.0)
    assert trimmed is True
    assert "## Entry 0" in excerpt
    assert "## Entry 5" in excerpt
    assert "## Entry 2" not in excerpt


def test_select_excerpt_cuts_a_single_section_at_the_budget() -> None:
    excerpt, trimmed = select_relevant_excerpt("word " * 500, "word", budget_chars=100)
    assert trimmed is True
    assert excerpt.startswith("word word")
    assert len(excerpt) < 200
