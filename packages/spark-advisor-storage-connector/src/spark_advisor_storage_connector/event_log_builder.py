from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from spark_advisor_models.model import JobAnalysis  # noqa: TC001
from spark_advisor_models.tracing import get_tracer
from spark_advisor_parser import parse_event_log

if TYPE_CHECKING:
    from spark_advisor_storage_connector.connectors.protocol import EventLogRef, StorageConnector


def _suffix_from_name(name: str) -> str:
    p = Path(name)
    suffixes = p.suffixes
    return "".join(suffixes) if suffixes else ".json"


async def fetch_and_parse_event_log(connector: StorageConnector, ref: EventLogRef) -> JobAnalysis:
    tracer = get_tracer()
    with tracer.start_as_current_span("storage.fetch_event_log", attributes={"path": ref.path, "size": ref.size}):
        raw_bytes = await connector.read_event_log(ref)

    suffix = _suffix_from_name(ref.name)
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(raw_bytes)
        tmp_path = Path(tmp.name)
    try:
        return parse_event_log(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
