"""SQL of one item (plan files v1): documents, passages, norm references, skipped items and coverage.

All statements run inside one transaction opened by the caller; the dataset schema is on the search_path (names are unqualified). The rules:

* a document is upserted on ``(fonte, source_id)``; its ``id`` survives and ``ingested_at`` moves only when something changed (a second ingest of the
  same plan is a no-op);
* passages are upserted on ``(documento_id, seq)``: the embedding is KEPT when ``text_contextual`` is unchanged and set NULL when it changed, and the
  passages beyond the current set are deleted;
* the references of a document are replaced (delete + insert) in the same transaction;
* ``skipped`` and ``coverage`` are upserted on their natural keys; a document that is now ingested clears its stale ``skipped`` row;
* a twin (the same text under another ``source_id``) is counted and kept: identity is ``(fonte, source_id)``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from dp_it_prassi_ingest.chunker import chunk_documento

if TYPE_CHECKING:
    from collections.abc import Iterable

PLAN_SCHEMA_VERSION = 1
TABLES = ("documenti", "passages", "riferimenti_norma", "skipped", "coverage")
FONTE_BY_KIND = {"ade": "ade-portale", "doctrib": "doctrib"}

DOCUMENTO_COLUMNS = ("ente", "struttura", "tipo", "numero", "anno", "data", "oggetto", "text_raw", "estrazione", "fonte", "source_id", "source_url")
_SET_ALL = ", ".join(f"{c} = EXCLUDED.{c}" for c in DOCUMENTO_COLUMNS)
_OLD = ", ".join(f"documenti.{c}" for c in DOCUMENTO_COLUMNS)
_NEW = ", ".join(f"EXCLUDED.{c}" for c in DOCUMENTO_COLUMNS)

UPSERT_DOCUMENTO = f"""
INSERT INTO documenti ({", ".join(DOCUMENTO_COLUMNS)}, ingested_at)
VALUES ({", ".join(["%s"] * len(DOCUMENTO_COLUMNS))}, now())
ON CONFLICT (fonte, source_id) DO UPDATE SET {_SET_ALL},
    ingested_at = CASE WHEN ({_OLD}) IS DISTINCT FROM ({_NEW}) THEN now() ELSE documenti.ingested_at END
RETURNING id
"""

# Text changed -> the embedding is stale: NULL it; identical contextual text keeps it.
UPSERT_PASSAGE = """
INSERT INTO passages (documento_id, seq, text, text_contextual) VALUES (%s, %s, %s, %s)
ON CONFLICT (documento_id, seq) DO UPDATE SET
    text = EXCLUDED.text,
    embedding = CASE WHEN passages.text_contextual IS DISTINCT FROM EXCLUDED.text_contextual THEN NULL ELSE passages.embedding END,
    text_contextual = EXCLUDED.text_contextual
"""
DELETE_SURPLUS_PASSAGES = "DELETE FROM passages WHERE documento_id = %s AND NOT (seq = ANY(%s::int[]))"

DELETE_RIFERIMENTI = "DELETE FROM riferimenti_norma WHERE documento_id = %s"
INSERT_RIFERIMENTO = (
    "INSERT INTO riferimenti_norma (documento_id, ord, descrizione, articolo, comma, norma_urn, via) VALUES (%s, %s, %s, %s, %s, %s, %s)"
)

CLEAR_SKIPPED = "DELETE FROM skipped WHERE fonte = %s AND item_ref = ANY(%s)"
UPSERT_SKIPPED = """
INSERT INTO skipped (fonte, item_ref, reason) VALUES (%s, %s, %s)
ON CONFLICT (fonte, item_ref) DO UPDATE SET reason = EXCLUDED.reason, seen_at = now()
"""
UPSERT_COVERAGE = """
INSERT INTO coverage (fonte, tipo, anno, mese, dichiarati, presi, mancanti, troncati) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (fonte, tipo, anno, mese) DO UPDATE SET
    dichiarati = EXCLUDED.dichiarati, presi = EXCLUDED.presi, mancanti = EXCLUDED.mancanti, troncati = EXCLUDED.troncati, noted_at = now()
"""
COUNT_TWINS = "SELECT count(*) FROM documenti WHERE md5(text_raw) = md5(%s) AND NOT (fonte = %s AND source_id = %s)"


class Cursor(Protocol):
    def execute(self, sql: Any, params: Any = None) -> Any: ...

    def executemany(self, sql: Any, params: Iterable[Any]) -> Any: ...


def write_documento(cursor: Cursor, item_id: str, plan: dict[str, Any]) -> dict[str, int]:
    """Writes the document of one plan file with its passages and references; returns the row counts and 1 in ``twins`` when it has a twin."""
    doc = plan["documento"]
    if not isinstance(doc["text_raw"], str) or not doc["text_raw"].strip():
        msg = f"{doc['source_id']}: empty text, the plan is not written"
        raise ValueError(msg)
    row = cursor.execute(UPSERT_DOCUMENTO, tuple(doc[c] for c in DOCUMENTO_COLUMNS)).fetchone()
    documento_id = row[0]
    pieces = chunk_documento(doc["text_raw"], doc["ente"], doc["tipo"], doc["numero"], doc["anno"])
    if pieces:
        cursor.executemany(UPSERT_PASSAGE, [(documento_id, p.seq, p.text, p.text_contextual) for p in pieces])
    cursor.execute(DELETE_SURPLUS_PASSAGES, (documento_id, [p.seq for p in pieces]))
    cursor.execute(DELETE_RIFERIMENTI, (documento_id,))
    refs = plan["riferimenti"]
    if refs:
        cursor.executemany(INSERT_RIFERIMENTO, [(documento_id, r["ord"], r["descrizione"], r["articolo"], r["comma"], r["norma_urn"], r["via"]) for r in refs])
    cursor.execute(CLEAR_SKIPPED, (doc["fonte"], [doc["source_id"], f"{item_id}/{doc['source_id']}"]))
    twins = cursor.execute(COUNT_TWINS, (doc["text_raw"], doc["fonte"], doc["source_id"])).fetchone()
    return {"documenti": 1, "passages": len(pieces), "riferimenti_norma": len(refs), "twins": 1 if twins and twins[0] else 0}


def coverage_row(coverage: dict[str, Any]) -> tuple[Any, ...]:
    """``(fonte, tipo, anno, mese, dichiarati, presi, mancanti, troncati)`` from the counts file of the grab (AdE: cards and documents; DocTrib: counters)."""
    if coverage["fonte"] == "doctrib":
        keys = ("fonte", "tipo", "anno", "mese", "dichiarati", "presi", "mancanti", "troncati")
        return tuple(coverage[k] for k in keys)
    declared = coverage["listed"] + coverage["senza_pdf"]
    missing = coverage["senza_pdf"] + coverage["dettagli_persi"]
    return (coverage["fonte"], coverage["tipo"], coverage["anno"], coverage["mese"], declared, coverage["documents"], missing, 0)


def write_skipped(cursor: Cursor, rows: list[tuple[str, str, str]]) -> int:
    if rows:
        cursor.executemany(UPSERT_SKIPPED, rows)
    return len(rows)


def write_coverage(cursor: Cursor, coverage: dict[str, Any]) -> int:
    cursor.execute(UPSERT_COVERAGE, coverage_row(coverage))
    return 1
