"""Passages of a prassi document (port of the branch-53 PrassiChunker).

No header is known, so every piece is a body piece: the sentence-boundary segmenter over ``text_raw`` with the contextual prefix of the estremi
(``[<ente> <tipo> n. <numero>/<anno>]``) in front of the embedded text. Pure, deterministic, lossless: ``"".join(p.text) == text_raw``.
"""
from __future__ import annotations

from dataclasses import dataclass

from dp_it_prassi_ingest.segmenter import segment


@dataclass(frozen=True)
class Piece:
    """One embeddable passage: ``text`` is stored and shown, ``text_contextual`` (prefix + text) is what gets embedded."""

    seq: int
    kind: str
    text: str
    text_contextual: str


def _prefix(ente: str, tipo: str, numero: str | None, anno: int | None) -> str:
    has_numero = numero is not None and bool(numero.strip())
    out = f"[{ente} {tipo}"
    if has_numero:
        out += f" n. {numero}"
    if anno is not None:
        out += ("/" if has_numero else " ") + str(anno)
    return out + "]\n"


def chunk_documento(text_raw: str, ente: str, tipo: str, numero: str | None, anno: int | None) -> list[Piece]:
    """The passages of one document, ``seq`` from 0; no passages for a blank text."""
    if not text_raw or not text_raw.strip():
        return []
    prefix = _prefix(ente, tipo, numero, anno)
    return [Piece(seq, "body", text, prefix + text) for seq, text in enumerate(segment(text_raw))]
