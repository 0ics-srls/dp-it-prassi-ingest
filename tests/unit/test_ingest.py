"""run_ingest with a fake connection: statement order, parameters, transactions, the embedding rule, skipped and coverage, metrics, secrets."""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pytest

from builders import LONG_TEXT, FakeConnection, documento, item, new_plan, riferimento, write_coverage, write_plan
from dp_it_prassi_ingest.errors import CONFIG_ERROR, SYSTEM_ERROR, TRANSIENT_ERROR, IngestError
from dp_it_prassi_ingest.ingest import run_ingest

CONN = "Host=db;Port=5432;Database=lex;Username=ds_it_prassi_rw;Password=s3cr3t-pw"
SCHEMA = "ds_it_prassi"
ITEM_ID = "ade:circolari:2025:07"


def params(directory: Path, *items: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"items": list(items), "dirIn": str(directory), "connString": CONN, "schema": SCHEMA, **extra}


def run(directory: Path, connection: FakeConnection, *items: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return run_ingest(params(directory, *items, **extra), lambda _dsn: connection, lambda *_: None)


def kinds(connection: FakeConnection) -> list[str]:
    return [" ".join(sql.split()[:3]) for _, sql, _ in connection.log]


def statement(connection: FakeConnection, text: str) -> tuple[str, str, Any]:
    return next(entry for entry in connection.log if text in entry[1])


@pytest.mark.unit
def test_a_document_item_runs_the_statements_in_order_inside_one_transaction(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "Circolare_n_10", new_plan(documento(), [riferimento()]))
    connection = FakeConnection()

    result = run(tmp_path, connection, item("ade", ITEM_ID))

    assert connection.events == ["begin", "commit"]
    assert kinds(connection) == [
        "SET search_path TO",
        "INSERT INTO documenti",
        "INSERT INTO passages",
        "DELETE FROM passages",
        "DELETE FROM riferimenti_norma",
        "INSERT INTO riferimenti_norma",
        "DELETE FROM skipped",
        "SELECT count(*) FROM",
    ]
    assert statement(connection, "SET search_path")[1] == f"SET search_path TO {SCHEMA}, public"
    assert connection.closed
    assert result["loaded"] == [f"{ITEM_ID}|v1"]
    assert result["metrics"][f"written.{SCHEMA}.documenti"] == 1
    assert result["metrics"][f"written.{SCHEMA}.passages"] == 1
    assert result["metrics"][f"written.{SCHEMA}.riferimenti_norma"] == 1


@pytest.mark.unit
def test_the_document_is_upserted_on_fonte_and_source_id_keeping_the_id(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "Circolare_n_10", new_plan(documento(tipo="circolare", numero="10", anno=2025)))
    connection = FakeConnection()

    run(tmp_path, connection, item("ade", ITEM_ID))

    _, sql, args = statement(connection, "INSERT INTO documenti")
    assert "ON CONFLICT (fonte, source_id) DO UPDATE" in sql
    assert "RETURNING id" in sql
    assert not re.search(r"(?<![a-z_])id = ", sql.split("DO UPDATE SET")[1]), "the id of an existing document is never replaced"
    assert args[0] == "Agenzia delle Entrate"
    assert args[2:6] == ("circolare", "10", 2025, "2025-07-03")


@pytest.mark.unit
def test_passages_keep_the_embedding_only_when_the_contextual_text_is_unchanged(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "Circolare_n_10", new_plan(documento()))
    connection = FakeConnection()

    run(tmp_path, connection, item("ade", ITEM_ID))

    _, sql, rows = statement(connection, "INSERT INTO passages")
    assert "ON CONFLICT (documento_id, seq) DO UPDATE" in sql
    assert "CASE WHEN passages.text_contextual IS DISTINCT FROM EXCLUDED.text_contextual THEN NULL ELSE passages.embedding END" in sql
    assert len(rows) == 1
    assert rows[0][1] == 0
    assert rows[0][3].startswith("[Agenzia delle Entrate circolare n. 10/2025]\n")


@pytest.mark.unit
def test_surplus_passages_of_the_document_are_deleted(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "Circolare_n_10", new_plan(documento(text_raw=LONG_TEXT)))
    connection = FakeConnection()

    run(tmp_path, connection, item("ade", ITEM_ID))

    _, _, rows = statement(connection, "INSERT INTO passages")
    _, sql, args = statement(connection, "DELETE FROM passages")
    assert len(rows) >= 3
    assert "NOT (seq = ANY" in sql
    assert args[1] == [r[1] for r in rows]


@pytest.mark.unit
def test_references_are_replaced_in_the_same_transaction(tmp_path: Path) -> None:
    refs = [riferimento(0), riferimento(1, descrizione="legge 27 luglio 2000, n. 212", articolo=None, norma_urn="urn:nir:stato:legge:2000-07-27;212", via="numero_data")]
    write_plan(tmp_path, ITEM_ID, "Circolare_n_10", new_plan(documento(), refs))
    connection = FakeConnection()

    run(tmp_path, connection, item("ade", ITEM_ID))

    assert kinds(connection).index("DELETE FROM riferimenti_norma") < kinds(connection).index("INSERT INTO riferimenti_norma")
    _, _, rows = statement(connection, "INSERT INTO riferimenti_norma")
    assert [r[1] for r in rows] == [0, 1]
    assert rows[1][5] == "urn:nir:stato:legge:2000-07-27;212"


@pytest.mark.unit
def test_a_plan_without_a_document_writes_only_the_skipped_row(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "scan", new_plan(None, skipped=[{"item_ref": "scan", "reason": "ocr_empty"}]))
    connection = FakeConnection()

    result = run(tmp_path, connection, item("ade", ITEM_ID))

    assert "INSERT INTO documenti" not in " ".join(kinds(connection))
    _, sql, rows = statement(connection, "INSERT INTO skipped")
    assert "ON CONFLICT (fonte, item_ref) DO UPDATE" in sql
    assert rows == [("ade-portale", "scan", "ocr_empty")]
    assert result["metrics"][f"written.{SCHEMA}.skipped"] == 1
    assert result["metrics"]["skipped_documents"] == 1


@pytest.mark.unit
def test_a_document_with_empty_text_is_refused_and_not_written(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "empty", new_plan(documento(text_raw="   ")))
    connection = FakeConnection()

    with pytest.raises(IngestError) as caught:
        run(tmp_path, connection, item("ade", ITEM_ID))

    assert caught.value.code == SYSTEM_ERROR
    assert "INSERT INTO documenti" not in " ".join(kinds(connection))
    assert connection.events == []


@pytest.mark.unit
def test_the_coverage_of_an_ade_item_is_upserted_from_the_counts_file(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "Circolare_n_10", new_plan(documento()))
    write_coverage(tmp_path, ITEM_ID, {
        "fonte": "ade-portale", "tipo": "circolare", "anno": 2025, "mese": 7, "cards": 3, "listed": 2, "documents": 2, "intestazioni": 1, "senza_pdf": 1,
        "dettagli_persi": 0, "pagina_404": False, "sample_filter": False,
        "skipped": [{"item_ref": f"{ITEM_ID}/Circolare n. 99 senza allegato", "reason": "senza_pdf"}],
    })
    connection = FakeConnection()

    result = run(tmp_path, connection, item("ade", ITEM_ID))

    _, sql, args = statement(connection, "INSERT INTO coverage")
    assert "ON CONFLICT (fonte, tipo, anno, mese) DO UPDATE" in sql
    assert args == ("ade-portale", "circolare", 2025, 7, 3, 2, 1, 0)
    _, _, skipped = statement(connection, "INSERT INTO skipped")
    assert skipped == [("ade-portale", f"{ITEM_ID}/Circolare n. 99 senza allegato", "senza_pdf")]
    assert result["metrics"][f"written.{SCHEMA}.coverage"] == 1


@pytest.mark.unit
def test_the_coverage_of_a_doctrib_item_carries_declared_taken_missing_and_truncated(tmp_path: Path) -> None:
    item_id = "doctrib:interpello:2025-10"
    write_plan(tmp_path, item_id, "x", new_plan(documento(fonte="doctrib", tipo="interpello", source_id="G1")))
    write_coverage(tmp_path, item_id, {"fonte": "doctrib", "tipo": "interpello", "anno": 2025, "mese": 10, "dichiarati": 17, "presi": 15, "mancanti": 2, "troncati": 1,
                                       "senza_pdf": 0, "sample_filter": False, "skipped": []})
    connection = FakeConnection()

    run(tmp_path, connection, item("doctrib", item_id))

    assert statement(connection, "INSERT INTO coverage")[2] == ("doctrib", "interpello", 2025, 10, 17, 15, 2, 1)


@pytest.mark.unit
def test_an_empty_month_still_writes_its_coverage(tmp_path: Path) -> None:
    write_coverage(tmp_path, ITEM_ID, {"fonte": "ade-portale", "tipo": "circolare", "anno": 2025, "mese": 10, "cards": 1, "listed": 0, "documents": 0, "intestazioni": 1,
                                       "senza_pdf": 0, "dettagli_persi": 0, "pagina_404": False, "sample_filter": False, "skipped": []})
    connection = FakeConnection()

    result = run(tmp_path, connection, item("ade", ITEM_ID))

    assert statement(connection, "INSERT INTO coverage")[2] == ("ade-portale", "circolare", 2025, 10, 0, 0, 0, 0)
    assert result["loaded"] == [f"{ITEM_ID}|v1"]


@pytest.mark.unit
def test_the_duplicate_text_counter_counts_a_twin_under_another_source_id(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento()))
    connection = FakeConnection(answers={"SELECT count(*) FROM": 1})

    result = run(tmp_path, connection, item("ade", ITEM_ID))

    assert result["metrics"]["duplicates_text"] == 1
    assert "md5(text_raw)" in statement(connection, "SELECT count(*) FROM")[1]


@pytest.mark.unit
def test_two_items_run_in_two_transactions_and_a_failing_one_rolls_back_alone(tmp_path: Path) -> None:
    other = "ade:risoluzioni:2025:10"
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento()))
    write_plan(tmp_path, other, "b", new_plan(documento(tipo="risoluzione", source_id="b")))
    connection = FakeConnection(fail_on="INSERT INTO riferimenti_norma")
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(), [riferimento()]))

    with pytest.raises(IngestError) as caught:
        run(tmp_path, connection, item("ade", ITEM_ID), item("ade", other))

    assert connection.events == ["begin", "rollback", "begin", "commit"]
    assert caught.value.code == SYSTEM_ERROR
    assert f"{ITEM_ID}|v1" in str(caught.value)
    assert "s3cr3t-pw" not in str(caught.value)


@pytest.mark.unit
def test_the_failure_message_and_the_log_never_carry_the_password(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento()))

    def refuse(_dsn: str) -> FakeConnection:
        raise OSError(CONN)

    with caplog.at_level(logging.DEBUG), pytest.raises(IngestError) as caught:
        run_ingest(params(tmp_path, item("ade", ITEM_ID)), refuse, lambda *_: None)

    assert caught.value.code == TRANSIENT_ERROR
    assert "s3cr3t-pw" not in str(caught.value)
    assert "s3cr3t-pw" not in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad",
    [{"items": []}, {"dirIn": ""}, {"connString": ""}, {"schema": "public"}, {"schema": "ds_x; DROP"}, {"items": [{"key": "k", "meta": {"kind": "gazzetta", "itemId": "x"}}]}],
)
def test_bad_parameters_are_config_errors(tmp_path: Path, bad: dict[str, Any]) -> None:
    merged = {**params(tmp_path, item("ade", ITEM_ID)), **bad}

    with pytest.raises(IngestError) as caught:
        run_ingest(merged, lambda _dsn: FakeConnection(), lambda *_: None)

    assert caught.value.code == CONFIG_ERROR


@pytest.mark.unit
def test_a_plan_that_breaks_the_schema_fails_the_item(tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(estrazione="scansione")))
    connection = FakeConnection()

    with pytest.raises(IngestError):
        run(tmp_path, connection, item("ade", ITEM_ID))

    assert connection.events == []
