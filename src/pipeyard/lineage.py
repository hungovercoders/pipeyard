"""OpenLineage (https://openlineage.io) events for pipeline runs and report apps.

Every run produces a START event and then exactly one COMPLETE or FAIL, as the standard
requires. Each event is always written to the org's prefix in S3, so lineage is on record
whatever else happens; if ``OPENLINEAGE_URL`` is set it is also POSTed there, in order, by a
background thread. Sending is best effort: an unreachable lineage server never fails a run.

The events are built by hand against spec 2-0-2 rather than with the OpenLineage client
library, to avoid a dependency for what is a handful of JSON documents.
"""

import json
import logging
import queue
import threading
import uuid
from datetime import UTC, datetime

import httpx

from . import contract
from .config import Settings
from .storage import Storage

PRODUCER = "https://github.com/hungovercoders/pipeyard"
SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
FACETS = "https://openlineage.io/spec/facets"
log = logging.getLogger("pipeyard.lineage")


def run_uuid(org_id: str, run_id: str) -> str:
    """OpenLineage run ids must be UUIDs. This one is derived, so it is stable and unique."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{PRODUCER}/orgs/{org_id}/runs/{run_id}"))


def _facet(version_and_name: str, **fields) -> dict:
    name = version_and_name.split("/")[-1]
    schema_url = f"{FACETS}/{version_and_name}.json#/$defs/{name}"
    return {"_producer": PRODUCER, "_schemaURL": schema_url, **fields}


def schema_facet(columns: list[dict]) -> dict:
    """``columns`` is a list of {name, type}, optionally with a description."""
    fields = [{k: c[k] for k in ("name", "type", "description") if c.get(k)} for c in columns]
    return _facet("1-2-0/SchemaDatasetFacet", fields=fields)


def statistics_facet(row_count: int) -> dict:
    return _facet("1-0-2/OutputStatisticsOutputDatasetFacet", rowCount=row_count)


def error_facet(message: str) -> dict:
    # The run is a container in any language, so there is no language to report.
    return _facet("1-0-1/ErrorMessageRunFacet", message=message, programmingLanguage="unknown")


def job_type_facet(job_type: str, processing: str = "BATCH") -> dict:
    return _facet(
        "2-0-4/JobTypeJobFacet", processingType=processing, integration="PIPEYARD", jobType=job_type
    )


class Lineage:
    def __init__(self, settings: Settings, storage: Storage):
        self.settings = settings
        self.storage = storage
        self._queue: queue.Queue | None = None

    # --- building -----------------------------------------------------------------------

    def dataset(self, org_id: str, name: str, facets: dict | None = None, **extra) -> dict:
        """A pipeyard dataset in OpenLineage's naming for S3: bucket as namespace, key as name."""
        key = contract.dataset_prefix(org_id, name).rstrip("/")
        return {
            "namespace": f"s3://{self.storage.bucket}",
            "name": key,
            "facets": facets or {},
            **extra,
        }

    def event(
        self,
        event_type: str,
        *,
        org_id: str,
        run_id: str,
        job: str,
        job_type: dict,
        when: datetime | None = None,
        inputs: list[dict] | None = None,
        outputs: list[dict] | None = None,
        run_facets: dict | None = None,
    ) -> dict:
        when = when or datetime.now(UTC)
        return {
            "eventType": event_type,
            "eventTime": when.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "producer": PRODUCER,
            "schemaURL": f"{SPEC}#/$defs/RunEvent",
            "run": {"runId": run_uuid(org_id, run_id), "facets": run_facets or {}},
            "job": {
                "namespace": f"{self.settings.openlineage_namespace}/{org_id}",
                "name": job,
                "facets": {"jobType": job_type},
            },
            "inputs": inputs or [],
            "outputs": outputs or [],
        }

    # --- emitting -----------------------------------------------------------------------

    def emit(self, org_id: str, run_id: str, event: dict) -> None:
        """Record the event in S3, then hand it to the sender if a server is configured."""
        try:
            self.storage.put_lineage_event(org_id, run_id, event)
        except Exception:  # noqa: BLE001 - lineage must never break the thing it describes
            log.exception("could not store lineage event for run %s", run_id)
        if self.settings.openlineage_url:
            if self._queue is None:
                self._queue = queue.Queue()
                threading.Thread(target=self._send_forever, daemon=True).start()
            self._queue.put(event)

    def _send_forever(self) -> None:
        url = (
            self.settings.openlineage_url.rstrip("/")
            + "/"
            + self.settings.openlineage_endpoint.lstrip("/")
        )
        headers = {"Content-Type": "application/json"}
        if self.settings.openlineage_api_key:
            headers["Authorization"] = f"Bearer {self.settings.openlineage_api_key}"
        with httpx.Client(timeout=5) as client:
            while True:
                event = self._queue.get()
                try:
                    client.post(url, content=json.dumps(event), headers=headers).raise_for_status()
                except httpx.HTTPError as e:
                    log.warning("lineage event not delivered to %s: %s", url, e)
                finally:
                    self._queue.task_done()

    def flush(self, timeout: float = 5) -> None:
        """Wait for queued events to be sent (used by tests)."""
        if self._queue is not None:
            done = threading.Event()
            threading.Thread(target=lambda: (self._queue.join(), done.set()), daemon=True).start()
            done.wait(timeout)
