"""
OpenLineage Event Emitter for Marquez
=====================================
Helper class that builds and sends OpenLineage-compliant events
to the Marquez API. Used by each pipeline stage to report lineage.
"""

import uuid
import requests
from datetime import datetime, timezone


class LineageEmitter:
    """Emits OpenLineage events to a Marquez backend."""

    def __init__(self, marquez_url="http://localhost:9000", namespace="smart-metering-pipeline"):
        self.api_url = f"{marquez_url}/api/v1/lineage"
        self.namespace = namespace
        self.producer = "https://github.com/prathameshpatil24/marquez-smart-metering-demo"
        self.schema_url = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

    def new_run_id(self):
        """Generate a new UUID for a pipeline run."""
        return str(uuid.uuid4())

    def _build_event(self, event_type, run_id, job_name, inputs=None, outputs=None,
                     job_facets=None, run_facets=None):
        """Build an OpenLineage event payload."""
        event = {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": run_id,
            },
            "job": {
                "namespace": self.namespace,
                "name": job_name,
            },
            "inputs": inputs or [],
            "outputs": outputs or [],
            "producer": self.producer,
            "schemaURL": self.schema_url,
        }

        if run_facets:
            event["run"]["facets"] = run_facets
        if job_facets:
            event["job"]["facets"] = job_facets

        return event

    def _send(self, event):
        """POST an OpenLineage event to Marquez."""
        try:
            resp = requests.post(self.api_url, json=event, timeout=10)
            resp.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"  ⚠ Failed to send lineage event: {e}")
            return False

    def emit_start(self, run_id, job_name, input_datasets=None):
        """Emit a START event when a job begins."""
        inputs = []
        if input_datasets:
            for ds in input_datasets:
                entry = {
                    "namespace": self.namespace,
                    "name": ds["name"],
                }
                if "facets" in ds:
                    entry["facets"] = ds["facets"]
                inputs.append(entry)

        event = self._build_event("START", run_id, job_name, inputs=inputs)
        return self._send(event)

    def emit_complete(self, run_id, job_name, input_datasets=None,
                      output_datasets=None, job_facets=None, run_facets=None):
        """Emit a COMPLETE event when a job succeeds."""
        inputs = []
        if input_datasets:
            for ds in input_datasets:
                inputs.append({"namespace": self.namespace, "name": ds["name"]})

        outputs = []
        if output_datasets:
            for ds in output_datasets:
                entry = {
                    "namespace": self.namespace,
                    "name": ds["name"],
                }
                if "facets" in ds:
                    entry["facets"] = ds["facets"]
                outputs.append(entry)

        event = self._build_event(
            "COMPLETE", run_id, job_name,
            inputs=inputs, outputs=outputs,
            job_facets=job_facets, run_facets=run_facets,
        )
        return self._send(event)

    def emit_fail(self, run_id, job_name, input_datasets=None,
                  error_message="", job_facets=None):
        """Emit a FAIL event when a job fails."""
        inputs = []
        if input_datasets:
            for ds in input_datasets:
                inputs.append({"namespace": self.namespace, "name": ds["name"]})

        run_facets = {
            "errorMessage": {
                "_producer": self.producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
                "message": error_message,
                "programmingLanguage": "Python",
            }
        }

        event = self._build_event(
            "FAIL", run_id, job_name,
            inputs=inputs,
            job_facets=job_facets,
            run_facets=run_facets,
        )
        return self._send(event)

    # ── Facet Builders ──────────────────────────────────────────────

    @staticmethod
    def schema_facet(fields):
        """Build a schema facet from a list of (name, type) tuples."""
        return {
            "schema": {
                "_producer": "https://github.com/prathameshpatil24/marquez-smart-metering-demo",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                "fields": [{"name": n, "type": t} for n, t in fields],
            }
        }

    @staticmethod
    def quality_facet(row_count, bytes_size=None):
        """Build a data quality metrics facet."""
        facet = {
            "dataQualityMetrics": {
                "_producer": "https://github.com/prathameshpatil24/marquez-smart-metering-demo",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
                "rowCount": row_count,
            }
        }
        if bytes_size:
            facet["dataQualityMetrics"]["bytes"] = bytes_size
        return facet

    @staticmethod
    def sql_facet(query):
        """Build a SQL job facet."""
        return {
            "sql": {
                "_producer": "https://github.com/prathameshpatil24/marquez-smart-metering-demo",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
                "query": query,
            }
        }

    @staticmethod
    def datasource_facet(name, uri):
        """Build a data source facet."""
        return {
            "dataSource": {
                "_producer": "https://github.com/prathameshpatil24/marquez-smart-metering-demo",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                "name": name,
                "uri": uri,
            }
        }

    @staticmethod
    def merge_facets(*facet_dicts):
        """Merge multiple facet dictionaries into one."""
        merged = {}
        for d in facet_dicts:
            merged.update(d)
        return merged
