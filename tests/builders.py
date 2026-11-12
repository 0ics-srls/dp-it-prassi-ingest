"""Test builders: plan files (schema v1), item entries and a fake psycopg connection that records statements and transaction boundaries."""
from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")
LONG_TEXT = "Il contribuente chiede chiarimenti sul trattamento fiscale della operazione descritta nell'istanza di interpello. " * 90


def safe_name(item_id: str) -> str:
    return _UNSAFE.sub("_", item_id.replace(" ", "-"))


def item(kind: str, item_id: str) -> dict[str, Any]:
    return {"key": f"{item_id}|v1", "meta": {"kind": kind, "itemId": item_id, "version": "1"}}


def documento(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "ente": "Agenzia delle Entrate",
        "struttura": None,
        "tipo": "circolare",
        "numero": "10",
        "anno": 2025,
        "data": "2025-07-03",
        "oggetto": "Tassazione dei redditi di lavoro dipendente",
        "text_raw": "CIRCOLARE N. 10/E. Il contribuente applica l'articolo 51 del TUIR. " * 3,
        "estrazione": "testo",
        "fonte": "ade-portale",
        "source_id": "Circolare_n_10_del_3_luglio_2025",
        "source_url": "https://www.agenziaentrate.gov.it/portale/documents/20143/1/Circolare_n_10.pdf/abc",
    }
    base.update(overrides)
    return base


def riferimento(ord_: int = 0, **overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "ord": ord_,
        "descrizione": "articolo 51 del TUIR",
        "articolo": "51",
        "comma": None,
        "norma_urn": "urn:nir:stato:decreto.del.presidente.della.repubblica:1986-12-22;917~art51",
        "via": "alias",
    }
    base.update(overrides)
    return base


def new_plan(doc: dict[str, Any] | None = None, refs: list[dict[str, Any]] | None = None, skipped: list[dict[str, str]] | None = None) -> dict[str, Any]:
    return {"version": 1, "documento": doc, "riferimenti": refs or [], "skipped": skipped or [], "warnings": []}


def write_plan(directory: Path, item_id: str, stem: str, plan: dict[str, Any]) -> Path:
    target = directory / safe_name(item_id)
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{stem}.plan.json"
    path.write_text(json.dumps(plan, ensure_ascii=False), encoding="utf-8")
    return path


def write_coverage(directory: Path, item_id: str, coverage: dict[str, Any]) -> None:
    target = directory / safe_name(item_id)
    target.mkdir(parents=True, exist_ok=True)
    (target / "_coverage.json").write_text(json.dumps(coverage), encoding="utf-8")


class FakeCursor:
    """Records (kind, sql, params); ``RETURNING id`` answers a deterministic uuid; ``fail_on`` makes the statement containing that text raise."""

    def __init__(self, log: list[tuple[str, str, Any]], fail_on: str | None, answers: dict[str, Any]) -> None:
        self._log = log
        self._fail_on = fail_on
        self._answers = answers
        self._last = ""

    def execute(self, sql: Any, params: Any = None) -> FakeCursor:
        text = str(sql)
        if self._fail_on and self._fail_on in text:
            msg = f"boom in {self._fail_on}"
            raise RuntimeError(msg)
        self._log.append(("execute", text, params))
        self._last = text
        return self

    def executemany(self, sql: Any, params: Any) -> None:
        if self._fail_on and self._fail_on in str(sql):
            msg = f"boom in {self._fail_on}"
            raise RuntimeError(msg)
        self._log.append(("executemany", str(sql), list(params)))

    def fetchone(self) -> tuple[Any, ...]:
        for marker, answer in self._answers.items():
            if marker in self._last:
                return (answer,) if not isinstance(answer, tuple) else answer
        return (uuid.UUID(int=len(self._log)),)


class FakeConnection:
    """Records statements and transaction boundaries."""

    def __init__(self, fail_on: str | None = None, answers: dict[str, Any] | None = None) -> None:
        self.log: list[tuple[str, str, Any]] = []
        self.events: list[str] = []
        self.closed = False
        self._fail_on = fail_on
        self._answers = {"SELECT count(*) FROM": 0, **(answers or {})}

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        self.events.append("commit")

    @contextmanager
    def cursor(self) -> Iterator[FakeCursor]:
        yield FakeCursor(self.log, self._fail_on, self._answers)

    def execute(self, sql: Any, params: Any = None) -> FakeCursor:
        return FakeCursor(self.log, self._fail_on, self._answers).execute(sql, params)

    def close(self) -> None:
        self.closed = True
