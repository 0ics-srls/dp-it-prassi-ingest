"""The legacy controls of the prassi corpus (branch 53, task-49) as SQL, run by the integration tests and reusable by a status check.

* UNICITA: no two documents with a number share ``(ente, tipo, numero, anno)`` (the English versions of a circolare carry a distinct identity in the source).
* CONTENUTO: no document without text (the table also refuses it by CHECK).
* COERENZA-DATA: the year of a document equals the year of its date.
* IDENTITA-DICHIARATA: where the act DECLARES its own number ("Risposta n. 118/2022", "CIRCOLARE N. 3/E") the stored number is that number; where it declares
  nothing the control abstains (1,509 legacy interpelli do not state their own number).

``ESTREMI_PATTERN`` is the very pattern of ``dp_it_prassi_convert.estremi.PATTERN_SQL`` (a test compares the two files): ingest and control must not use two
regular expressions. The statements address unqualified tables (the dataset schema is on the search_path).
"""
from __future__ import annotations

ESTREMI_PATTERN = r"(?:Risposta|CIRCOLARE|RISOLUZIONE|Interpello|Prot\.)\s*n\.?\s*\*{0,2}\s*(\d{1,7})\s*(?:/\s*(\d{4}|[A-Z]))?"

UNICITA_SQL = (
    "SELECT ente, tipo, numero, anno, count(*) AS copies FROM documenti WHERE numero IS NOT NULL GROUP BY ente, tipo, numero, anno HAVING count(*) > 1"
)
CONTENUTO_SQL = "SELECT count(*) FROM documenti WHERE length(btrim(text_raw)) = 0"
COERENZA_DATA_SQL = "SELECT count(*) FROM documenti WHERE data IS NOT NULL AND anno IS NOT NULL AND anno <> extract(year FROM data)::int"
IDENTITA_DICHIARATA_SQL = (
    "SELECT count(*) FROM documenti WHERE text_raw ~* %s AND numero IS DISTINCT FROM ((regexp_match(text_raw, %s, 'i'))[1]::bigint)::text"
)
