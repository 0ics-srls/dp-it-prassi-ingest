"""The ``execute`` action: loads the plan files of the given items (``<dirIn>/<item dir>/*.plan.json`` and ``_coverage.json``), one transaction per item."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from jsonschema import Draft202012Validator

from dp_it_prassi_ingest.db import role_of
from dp_it_prassi_ingest.errors import CONFIG_ERROR, SYSTEM_ERROR, TRANSIENT_ERROR, DbConnectError, IngestError
from dp_it_prassi_ingest.repository import FONTE_BY_KIND, TABLES, write_coverage, write_documento, write_skipped

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

    from dp_it_prassi_ingest.repository import Cursor

KINDS = tuple(FONTE_BY_KIND)
_SCHEMA = re.compile(r"^ds_[a-z0-9_]+$")
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


class Connection(Protocol):
    def transaction(self) -> AbstractContextManager[Any]: ...

    def cursor(self) -> AbstractContextManager[Cursor]: ...

    def execute(self, sql: Any, params: Any = None) -> Any: ...

    def close(self) -> None: ...


def safe_name(item_id: str) -> str:
    """Directory name of an item id (identical to the grab and convert classes)."""
    return _UNSAFE.sub("_", item_id.replace(" ", "-"))


def _plan_validator() -> Draft202012Validator:
    """The plan schema shipped next to main.py (repo root, /app in the image); read when first needed, never at import."""
    path = Path(__file__).resolve().parents[2] / "plan.schema.json"
    return Draft202012Validator(json.loads(path.read_text(encoding="utf-8")))


def _validate(params: dict[str, Any]) -> tuple[list[dict[str, Any]], Path, str, str]:
    items = params.get("items")
    if not isinstance(items, list) or not items:
        raise IngestError(CONFIG_ERROR, "items must be a non-empty array")
    for entry in items:
        meta = entry.get("meta") if isinstance(entry, dict) else None
        if not isinstance(entry, dict) or not entry.get("key") or not isinstance(meta, dict) or meta.get("kind") not in KINDS:
            raise IngestError(CONFIG_ERROR, f"item needs a key and meta.kind one of {list(KINDS)}: {entry!r}")
        if not isinstance(meta.get("itemId"), str) or not meta["itemId"]:
            raise IngestError(CONFIG_ERROR, f"item {entry['key']}: meta.itemId is required")
    directory, conn_string, schema = params.get("dirIn"), params.get("connString"), params.get("schema")
    if not isinstance(directory, str) or not directory:
        raise IngestError(CONFIG_ERROR, "dirIn is required")
    if not isinstance(conn_string, str) or not conn_string:
        raise IngestError(CONFIG_ERROR, "connString is required")
    if not isinstance(schema, str) or not _SCHEMA.match(schema):
        raise IngestError(CONFIG_ERROR, "schema must be a dataset schema name (ds_...)")
    return items, Path(directory), conn_string, schema


def _load(entry: dict[str, Any], directory: Path) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """The validated plan files of the item (in file name order) and its counts file, before anything touches the database."""
    item_dir = directory / safe_name(entry["meta"]["itemId"])
    if not item_dir.is_dir():
        msg = f"no plan directory for the item at {item_dir}"
        raise FileNotFoundError(msg)
    validator = _plan_validator()
    plans: list[dict[str, Any]] = []
    for path in sorted(item_dir.glob("*.plan.json")):
        plan = json.loads(path.read_text(encoding="utf-8"))
        errors = sorted(validator.iter_errors(plan), key=lambda e: list(e.path))
        if errors:
            msg = f"{path.name} breaks the plan schema: {errors[0].message[:200]}"
            raise ValueError(msg)
        plans.append(plan)
    coverage_path = item_dir / "_coverage.json"
    coverage = json.loads(coverage_path.read_text(encoding="utf-8")) if coverage_path.is_file() else None
    return plans, coverage


def _write(cursor: Cursor, entry: dict[str, Any], plans: list[dict[str, Any]], coverage: dict[str, Any] | None) -> dict[str, int]:
    item_id = entry["meta"]["itemId"]
    fonte = FONTE_BY_KIND[entry["meta"]["kind"]]
    counts = dict.fromkeys((*TABLES, "twins", "skipped_documents", "warnings"), 0)
    skipped_rows: list[tuple[str, str, str]] = []
    for plan in plans:
        if plan["documento"] is not None:
            for name, value in write_documento(cursor, item_id, plan).items():
                counts[name] += value
        skipped_rows.extend((fonte, s["item_ref"], s["reason"]) for s in plan["skipped"])
        counts["skipped_documents"] += len(plan["skipped"])
        counts["warnings"] += len(plan["warnings"])
    if coverage is not None:
        skipped_rows.extend((coverage["fonte"], s["item_ref"], s["reason"]) for s in coverage.get("skipped", []))
        counts["coverage"] += write_coverage(cursor, coverage)
    counts["skipped"] += write_skipped(cursor, skipped_rows)
    return counts


def _items(
    connection: Connection, schema: str, items: list[dict[str, Any]], directory: Path, on_progress: Callable[[int, str], None],
) -> Iterator[tuple[dict[str, Any], dict[str, int] | Exception]]:
    for index, entry in enumerate(items):
        try:
            plans, coverage = _load(entry, directory)
            with connection.transaction():
                connection.execute(f"SET search_path TO {schema}, public")  # schema is validated against ^ds_[a-z0-9_]+$
                with connection.cursor() as cursor:
                    counts = _write(cursor, entry, plans, coverage)
            yield entry, counts
        except Exception as error:  # noqa: BLE001 - any failure of one item is reported, the next item still runs
            yield entry, error
        on_progress(int((index + 1) * 100 / len(items)), f"{index + 1}/{len(items)} {entry['key']}")


def run_ingest(params: dict[str, Any], connect: Callable[[str], Connection], on_progress: Callable[[int, str], None]) -> dict[str, Any]:
    """Ingests every item; failed items are rolled back and reported together as one system_error after the good ones were committed."""
    items, directory, conn_string, schema = _validate(params)
    try:
        connection = connect(conn_string)
    except (OSError, DbConnectError) as ex:
        msg = f"cannot connect to the dataset database as role {role_of(conn_string)} ({type(ex).__name__})"
        raise IngestError(TRANSIENT_ERROR, msg) from None
    metrics: dict[str, int] = {"items": 0, "skipped_documents": 0, "warnings": 0, "duplicates_text": 0}
    metrics.update({f"written.{schema}.{table}": 0 for table in TABLES})
    loaded: list[str] = []
    failures: list[str] = []
    try:
        for entry, outcome in _items(connection, schema, items, directory, on_progress):
            if isinstance(outcome, Exception):
                failures.append(f"{entry['key']} ({type(outcome).__name__}: {outcome})")
                continue
            metrics["items"] += 1
            metrics["skipped_documents"] += outcome["skipped_documents"]
            metrics["warnings"] += outcome["warnings"]
            metrics["duplicates_text"] += outcome["twins"]
            for table in TABLES:
                metrics[f"written.{schema}.{table}"] += outcome[table]
            loaded.append(entry["key"])
    finally:
        connection.close()
    if failures:
        msg = f"{len(failures)} of {len(items)} items failed: {'; '.join(failures)}"
        raise IngestError(SYSTEM_ERROR, msg)
    return {"metrics": metrics, "loaded": loaded}
