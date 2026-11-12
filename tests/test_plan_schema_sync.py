"""plan.schema.json is one file kept in two class repos (convert writes the plan, ingest reads it), and the estremi pattern of the SQL control is the
very string convert uses at ingest time: both must be equal to their twin.

The sibling repo is looked up lazily; where it is absent (the ProcMon test container holds this repo only) the comparison is skipped and the files are
still checked on their own.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from dp_it_prassi_ingest import controls


def _own() -> Path:
    return Path(__file__).resolve().parent.parent / "plan.schema.json"


def _sibling(relative: str) -> Path | None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "dp-it-prassi-convert" / relative
        if candidate.is_file():
            return candidate
    return None


@pytest.mark.unit
def test_the_schema_is_valid() -> None:
    Draft202012Validator.check_schema(json.loads(_own().read_text(encoding="utf-8")))


@pytest.mark.unit
def test_the_two_repos_carry_the_same_plan_schema() -> None:
    other = _sibling("plan.schema.json")
    if other is None:
        pytest.skip("dp-it-prassi-convert is not next to this repo")

    assert json.loads(other.read_text(encoding="utf-8")) == json.loads(_own().read_text(encoding="utf-8"))


@pytest.mark.unit
def test_the_sql_control_uses_the_estremi_pattern_of_convert() -> None:
    other = _sibling("src/dp_it_prassi_convert/estremi.py")
    if other is None:
        pytest.skip("dp-it-prassi-convert is not next to this repo")

    declared = re.search(r'^PATTERN_SQL = r"(.*)"$', other.read_text(encoding="utf-8"), re.MULTILINE)

    assert declared is not None
    assert declared.group(1) == controls.ESTREMI_PATTERN
