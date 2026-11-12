"""PrassiChunker port: contextual prefix with the estremi, every piece a body piece, lossless, deterministic; golden against the C# chunker of branch 53."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from dp_it_prassi_ingest.chunker import Piece, chunk_documento
from dp_it_prassi_ingest.segmenter import MAX_CHARS

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
ENTE = "Agenzia delle Entrate"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("numero", "anno", "prefix"),
    [
        ("6/E", 2025, "[Agenzia delle Entrate circolare n. 6/E/2025]\n"),
        (None, 2025, "[Agenzia delle Entrate circolare 2025]\n"),
        ("274", None, "[Agenzia delle Entrate circolare n. 274]\n"),
        (None, None, "[Agenzia delle Entrate circolare]\n"),
    ],
)
def test_prefix_variants(numero: str | None, anno: int | None, prefix: str) -> None:
    pieces = chunk_documento("Il testo del documento.", ENTE, "circolare", numero, anno)

    assert [p.text_contextual for p in pieces] == [prefix + "Il testo del documento."]
    assert "  " not in pieces[0].text_contextual
    assert "/]" not in pieces[0].text_contextual


@pytest.mark.unit
def test_every_piece_is_a_body_piece_and_nothing_is_lost() -> None:
    text = "".join(f"Il paragrafo {i} della circolare tratta un profilo fiscale specifico. " for i in range(1, 41))

    pieces = chunk_documento(text, ENTE, "circolare", "6/E", 2025)

    assert pieces
    assert {p.kind for p in pieces} == {"body"}
    assert "".join(p.text for p in pieces) == text
    assert [p.seq for p in pieces] == list(range(len(pieces)))


@pytest.mark.unit
def test_chunks_are_deterministic() -> None:
    text = "Una frase di prova abbastanza lunga da riempire il pezzo. " * 200

    assert chunk_documento(text, ENTE, "risoluzione", "9", 2024) == chunk_documento(text, ENTE, "risoluzione", "9", 2024)


@pytest.mark.unit
@pytest.mark.parametrize("empty", ["", "   ", "\n\n"])
def test_empty_text_gives_no_chunks(empty: str) -> None:
    assert chunk_documento(empty, ENTE, "circolare", "1", 2025) == []


@pytest.mark.unit
def test_every_piece_within_max_chars() -> None:
    pieces = chunk_documento("x" * 30_000, ENTE, "circolare", "1", 2025)

    assert len(pieces) == 4
    assert all(len(p.text) <= MAX_CHARS for p in pieces)


@pytest.mark.unit
def test_piece_is_a_value_object() -> None:
    assert Piece(0, "body", "a", "[x]\na") == Piece(0, "body", "a", "[x]\na")


@pytest.mark.unit
def test_should_cut_the_real_circolare_exactly_like_the_c_sharp_chunker_of_branch_53() -> None:
    golden = json.loads((FIXTURES / "chunk-golden.json").read_text(encoding="utf-8"))
    text = (FIXTURES / "chunk-input.txt").read_bytes().decode("utf-8")
    assert _sha(text) == golden["source_text_sha256"]
    assert len(text) == golden["source_text_len"]

    pieces = chunk_documento(text, golden["ente"], golden["tipo"], golden["numero"], golden["anno"])

    assert len(pieces) == len(golden["pieces"]) == 11
    for piece, expected in zip(pieces, golden["pieces"], strict=True):
        assert (piece.seq, piece.kind, len(piece.text)) == (expected["seq"], expected["kind"], expected["text_len"])
        assert _sha(piece.text) == expected["text_sha256"]
        assert _sha(piece.text_contextual) == expected["contextual_sha256"]
