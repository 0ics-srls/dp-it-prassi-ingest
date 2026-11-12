"""Real Postgres + pgvector (Testcontainers) with the package migrations applied; no Jina call (embeddings are set by SQL)."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from builders import LONG_TEXT, documento, item, new_plan, riferimento, write_coverage, write_plan
from dp_it_prassi_ingest import controls
from dp_it_prassi_ingest.db import connect
from dp_it_prassi_ingest.errors import IngestError
from dp_it_prassi_ingest.ingest import run_ingest

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

SCHEMA = "ds_it_prassi"
VECTOR = "[" + ",".join(["0.25"] * 1024) + "]"
ITEM_ID = "ade:circolari:2025:07"
DOCTRIB_ID = "doctrib:interpello:2025-10"


def migrations_dir() -> Path:
    """The package migrations next to this class repo; resolved lazily so collecting the module in a container (no sibling repo) cannot fail."""
    for parent in Path(__file__).resolve().parents:
        for candidate in (parent / "dp-it-prassi" / "schema" / "migrations", parent.parent / "dp-it-prassi" / "schema" / "migrations"):
            if candidate.is_dir():
                return candidate
    msg = "package migrations (dp-it-prassi/schema/migrations) not found next to this repo"
    raise FileNotFoundError(msg)


class Db:
    def __init__(self, container: PostgresContainer) -> None:
        self.dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        self.conn_string = (
            f"Host={container.get_container_host_ip()};Port={container.get_exposed_port(5432)};"
            f"Database={container.dbname};Username={container.username};Password={container.password}"
        )

    def scalar(self, sql: str) -> Any:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(sql).fetchone()
            assert row is not None
            return row[0]

    def execute(self, sql: str) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(sql)

    def ingest(self, directory: Path, *items: dict[str, Any]) -> dict[str, Any]:
        params = {"items": list(items), "dirIn": str(directory), "connString": self.conn_string, "schema": SCHEMA}
        return run_ingest(params, connect, lambda *_: None)

    def count(self, table: str, where: str = "true") -> int:
        return int(self.scalar(f"SELECT count(*) FROM {SCHEMA}.{table} WHERE {where}"))


@pytest.fixture(scope="module")
def container() -> Iterator[PostgresContainer]:
    with PostgresContainer("pgvector/pgvector:pg17", username="lex", password="pw", dbname="lex") as pg:
        yield pg


@pytest.fixture
def db(container: PostgresContainer) -> Db:
    database = Db(container)
    database.execute("CREATE EXTENSION IF NOT EXISTS vector")
    database.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    database.execute(f"CREATE SCHEMA {SCHEMA}")
    with psycopg.connect(database.dsn, autocommit=True) as conn:
        conn.execute(f"SET search_path TO {SCHEMA}, public")
        for migration in sorted(migrations_dir().glob("*.sql")):
            conn.execute(migration.read_text(encoding="utf-8"))
    return database


def test_first_ingest_writes_document_passages_and_references_with_the_act_urn(db: Db, tmp_path: Path) -> None:
    refs = [riferimento(0), riferimento(1, descrizione="legge 27 luglio 2000, n. 212", articolo=None, norma_urn="urn:nir:stato:legge:2000-07-27;212", via="numero_data")]
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(tipo="circolare"), refs))

    result = db.ingest(tmp_path, item("ade", ITEM_ID))

    assert result["metrics"][f"written.{SCHEMA}.documenti"] == 1
    assert db.count("documenti") == 1
    assert db.count("passages") == 1
    assert db.count("riferimenti_norma") == 2
    assert db.scalar(f"SELECT tipo FROM {SCHEMA}.documenti") == "circolare"
    assert db.scalar(f"SELECT norma_act_urn FROM {SCHEMA}.riferimenti_norma WHERE ord = 0") == "urn:nir:stato:decreto.del.presidente.della.repubblica:1986-12-22;917"


def test_second_ingest_of_the_same_plan_is_a_no_op_and_keeps_the_embeddings(db: Db, tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(text_raw=LONG_TEXT), [riferimento()]))
    db.ingest(tmp_path, item("ade", ITEM_ID))
    db.execute(f"UPDATE {SCHEMA}.passages SET embedding = '{VECTOR}'")
    stamp = db.scalar(f"SELECT ingested_at FROM {SCHEMA}.documenti")
    ident = db.scalar(f"SELECT id FROM {SCHEMA}.documenti")
    passages = db.count("passages")

    db.ingest(tmp_path, item("ade", ITEM_ID))

    assert db.count("documenti") == 1
    assert db.scalar(f"SELECT id FROM {SCHEMA}.documenti") == ident
    assert db.scalar(f"SELECT ingested_at FROM {SCHEMA}.documenti") == stamp, "an unchanged document keeps its ingest time"
    assert db.count("passages") == passages
    assert db.count("passages", "embedding IS NOT NULL") == passages
    assert db.count("riferimenti_norma") == 1


def test_changed_text_nulls_the_embedding_and_shrinks_the_passages(db: Db, tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(text_raw=LONG_TEXT)))
    db.ingest(tmp_path, item("ade", ITEM_ID))
    db.execute(f"UPDATE {SCHEMA}.passages SET embedding = '{VECTOR}'")
    before = db.count("passages")
    assert before >= 3

    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(text_raw="Testo molto piu breve del documento, che ora ha un solo passaggio.")))
    db.ingest(tmp_path, item("ade", ITEM_ID))

    assert db.count("passages") == 1
    assert db.count("passages", "embedding IS NULL") == 1
    assert db.count("documenti") == 1


def test_a_provvedimento_without_number_year_and_date_inserts(db: Db, tmp_path: Path) -> None:
    doc = documento(tipo="provvedimento", numero=None, anno=None, data=None, oggetto=None, source_id="Provv_senza_numero")
    write_plan(tmp_path, ITEM_ID, "p", new_plan(doc))

    db.ingest(tmp_path, item("ade", ITEM_ID))

    assert db.count("documenti", "numero IS NULL AND anno IS NULL AND data IS NULL") == 1


def test_a_failing_item_rolls_back_and_the_others_commit(db: Db, tmp_path: Path) -> None:
    broken = documento(source_id="broken", text_raw="testo con byte nullo \u0000 che postgres rifiuta")
    write_plan(tmp_path, ITEM_ID, "good", new_plan(documento(source_id="good-in-broken-item")))
    write_plan(tmp_path, ITEM_ID, "zbroken", new_plan(broken))
    write_plan(tmp_path, "ade:risoluzioni:2025:10", "ok", new_plan(documento(tipo="risoluzione", source_id="ok")))

    with pytest.raises(IngestError) as caught:
        db.ingest(tmp_path, item("ade", ITEM_ID), item("ade", "ade:risoluzioni:2025:10"))

    assert f"{ITEM_ID}|v1" in str(caught.value)
    assert db.count("documenti", "source_id = 'good-in-broken-item'") == 0, "the whole item rolled back"
    assert db.count("documenti", "source_id = 'ok'") == 1


def test_two_items_in_one_run(db: Db, tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(source_id="a")))
    write_plan(tmp_path, DOCTRIB_ID, "b", new_plan(documento(fonte="doctrib", tipo="interpello", source_id="G-1", numero="274", anno=2025)))

    result = db.ingest(tmp_path, item("ade", ITEM_ID), item("doctrib", DOCTRIB_ID))

    assert result["loaded"] == [f"{ITEM_ID}|v1", f"{DOCTRIB_ID}|v1"]
    assert db.count("documenti") == 2
    assert db.count("documenti", "fonte = 'doctrib'") == 1


def test_the_duplicate_text_counter_counts_a_twin_and_keeps_both(db: Db, tmp_path: Path) -> None:
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(source_id="one", numero="1")))
    db.ingest(tmp_path, item("ade", ITEM_ID))
    write_plan(tmp_path, ITEM_ID, "b", new_plan(documento(source_id="two", numero="2")))

    result = db.ingest(tmp_path, item("ade", ITEM_ID))

    assert result["metrics"]["duplicates_text"] == 1
    assert db.count("documenti") == 2


def test_a_scan_that_failed_ocr_is_a_skipped_row_and_a_later_success_clears_it(db: Db, tmp_path: Path) -> None:
    write_plan(tmp_path, DOCTRIB_ID, "scan", new_plan(None, skipped=[{"item_ref": "scan", "reason": "ocr_empty"}]))
    db.ingest(tmp_path, item("doctrib", DOCTRIB_ID))
    assert db.count("skipped", "reason = 'ocr_empty'") == 1
    assert db.count("documenti") == 0

    write_coverage(tmp_path, DOCTRIB_ID, {"fonte": "doctrib", "tipo": "interpello", "anno": 2025, "mese": 10, "dichiarati": 1, "presi": 0, "mancanti": 1, "troncati": 0,
                                          "senza_pdf": 1, "sample_filter": False, "skipped": [{"item_ref": f"{DOCTRIB_ID}/G-9", "reason": "senza_pdf"}]})
    db.ingest(tmp_path, item("doctrib", DOCTRIB_ID))
    assert db.count("skipped") == 2
    assert db.count("coverage", "fonte = 'doctrib' AND dichiarati = 1 AND mancanti = 1") == 1

    (tmp_path / "doctrib_interpello_2025-10" / "scan.plan.json").unlink()
    # the retried grab found the PDF: its counts file no longer lists the document as lost
    write_coverage(tmp_path, DOCTRIB_ID, {"fonte": "doctrib", "tipo": "interpello", "anno": 2025, "mese": 10, "dichiarati": 1, "presi": 1, "mancanti": 0, "troncati": 0,
                                          "senza_pdf": 0, "sample_filter": False, "skipped": []})
    write_plan(tmp_path, DOCTRIB_ID, "G-9", new_plan(documento(fonte="doctrib", tipo="interpello", source_id="G-9", numero="9")))
    db.ingest(tmp_path, item("doctrib", DOCTRIB_ID))
    assert db.count("skipped", "item_ref = 'doctrib:interpello:2025-10/G-9'") == 0


def test_the_legacy_controls_as_sql(db: Db, tmp_path: Path) -> None:
    good_text = "Risposta n. 267/2024 OGGETTO: IVA. Il contribuente applica la disciplina. " * 4
    write_plan(tmp_path, ITEM_ID, "a", new_plan(documento(source_id="a", tipo="interpello", numero="267", anno=2024, data="2024-12-23", text_raw=good_text)))
    write_plan(tmp_path, ITEM_ID, "b", new_plan(documento(source_id="b", tipo="provvedimento", numero=None, anno=2025, data="2025-10-31", text_raw="Provvedimento del Direttore senza numero in testa. " * 4)))
    write_plan(tmp_path, ITEM_ID, "c", new_plan(documento(source_id="c", tipo="circolare", numero="10", anno=2025, data="2025-07-03")))
    db.ingest(tmp_path, item("ade", ITEM_ID))

    unique = db.scalar(f"SELECT count(*) FROM ({controls.UNICITA_SQL.replace('FROM documenti', f'FROM {SCHEMA}.documenti')}) t")
    coerenza = db.scalar(controls.COERENZA_DATA_SQL.replace("FROM documenti", f"FROM {SCHEMA}.documenti"))
    declared = db.scalar(controls.IDENTITA_DICHIARATA_SQL.replace("FROM documenti", f"FROM {SCHEMA}.documenti").replace("%s", "'" + controls.ESTREMI_PATTERN + "'"))

    assert (unique, coerenza, declared) == (0, 0, 0)
    # the control sees a real disagreement and abstains where the act declares nothing
    db.execute(f"UPDATE {SCHEMA}.documenti SET numero = '999' WHERE source_id = 'a'")
    disagreement = db.scalar(controls.IDENTITA_DICHIARATA_SQL.replace("FROM documenti", f"FROM {SCHEMA}.documenti").replace("%s", "'" + controls.ESTREMI_PATTERN + "'"))
    assert disagreement == 1
    # two documents with the same identity (a scan stored under two source ids) are what the uniqueness control reports
    write_plan(tmp_path, ITEM_ID, "d", new_plan(documento(source_id="d", tipo="circolare", numero="10", anno=2025, data="2025-07-03", text_raw="Altro testo della stessa circolare. " * 5)))
    db.ingest(tmp_path, item("ade", ITEM_ID))
    assert db.scalar(f"SELECT count(*) FROM ({controls.UNICITA_SQL.replace('FROM documenti', f'FROM {SCHEMA}.documenti')}) t") == 1
