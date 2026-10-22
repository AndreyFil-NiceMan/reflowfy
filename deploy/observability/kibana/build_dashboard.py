"""Build kibana-saved-objects.ndjson: the Reflowfy logs dashboard (Kibana 8.11+).

    python deploy/observability/kibana/build_dashboard.py

Import the result in Kibana: Stack Management -> Saved Objects -> Import.
Fields it uses are listed in docs/logging.md; apply index-template.json first so
they are keyword/number fields.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

DATA_VIEW = "reflowfy-logs"
LAYER = "l1"
KQL = "language"


def _col_count() -> Dict[str, Any]:
    return {
        "label": "Events",
        "customLabel": True,
        "dataType": "number",
        "operationType": "count",
        "isBucketed": False,
        "scale": "ratio",
        "sourceField": "___records___",
        "params": {"emptyAsNull": False},
    }


def _col_sum(field: str) -> Dict[str, Any]:
    return {
        "label": f"Sum of {field}",
        "dataType": "number",
        "operationType": "sum",
        "isBucketed": False,
        "scale": "ratio",
        "sourceField": field,
        "params": {"emptyAsNull": True},
    }


def _col_pct(field: str, pct: int) -> Dict[str, Any]:
    return {
        "label": f"p{pct} {field}",
        "dataType": "number",
        "operationType": "percentile",
        "isBucketed": False,
        "scale": "ratio",
        "sourceField": field,
        "params": {"percentile": pct},
    }


def _col_time() -> Dict[str, Any]:
    return {
        "label": "@timestamp",
        "dataType": "date",
        "operationType": "date_histogram",
        "sourceField": "@timestamp",
        "isBucketed": True,
        "scale": "interval",
        "params": {"interval": "auto", "includeEmptyIntervals": True, "dropPartials": False},
    }


def _col_terms(field: str, size: int = 10, order_by: str = "count") -> Dict[str, Any]:
    return {
        "label": f"Top values of {field}",
        "dataType": "string",
        "operationType": "terms",
        "scale": "ordinal",
        "sourceField": field,
        "isBucketed": True,
        "params": {
            "size": size,
            "orderBy": {"type": "column", "columnId": order_by},
            "orderDirection": "desc",
            "otherBucket": True,
            "missingBucket": False,
            "parentFormat": {"id": "terms"},
        },
    }


def _lens(
    title: str,
    vis_type: str,
    columns: Dict[str, Dict[str, Any]],
    visualization: Dict[str, Any],
    query: str,
) -> Dict[str, Any]:
    return {
        "type": "lens",
        "attributes": {
            "title": title,
            "visualizationType": vis_type,
            "state": {
                "visualization": visualization,
                "query": {"query": query, KQL: "kuery"},
                "filters": [],
                "datasourceStates": {
                    "formBased": {
                        "layers": {
                            LAYER: {
                                "columns": columns,
                                "columnOrder": list(columns),
                                "incompleteColumns": {},
                            }
                        }
                    }
                },
                "internalReferences": [],
                "adHocDataViews": {},
            },
        },
        "references": [
            {
                "type": "index-pattern",
                "id": DATA_VIEW,
                "name": f"indexpattern-datasource-layer-{LAYER}",
            }
        ],
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "8.9.0",
    }


def bars(
    title: str,
    query: str,
    y: Dict[str, Any],
    split: Optional[str] = None,
    kind: str = "bar_stacked",
) -> Dict[str, Any]:
    columns: Dict[str, Dict[str, Any]] = {"x": _col_time()}
    if split:
        columns["split"] = _col_terms(split, order_by="y")
    columns["y"] = y
    layer: Dict[str, Any] = {
        "layerId": LAYER,
        "layerType": "data",
        "seriesType": kind,
        "accessors": ["y"],
        "xAccessor": "x",
        "position": "top",
        "showGridlines": False,
    }
    if split:
        layer["splitAccessor"] = "split"
    vis = {
        "legend": {"isVisible": True, "position": "right"},
        "valueLabels": "hide",
        "preferredSeriesType": kind,
        "layers": [layer],
        "axisTitlesVisibilitySettings": {"x": False, "yLeft": False, "yRight": False},
    }
    return _lens(title, "lnsXY", columns, vis, query)


def metric(title: str, query: str, y: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    columns = {"m": y or _col_count()}
    vis = {"layerId": LAYER, "layerType": "data", "metricAccessor": "m"}
    return _lens(title, "lnsMetric", columns, vis, query)


def table(title: str, query: str, groups: List[str], y: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    columns: Dict[str, Dict[str, Any]] = {f"g{i}": _col_terms(f, 20, "count") for i, f in enumerate(groups)}
    # terms are ordered by the metric column, which must be called "count"
    columns["count"] = y or _col_count()
    vis = {
        "layerId": LAYER,
        "layerType": "data",
        "columns": [{"columnId": c, "isTransposed": False} for c in columns],
    }
    return _lens(title, "lnsDatatable", columns, vis, query)


JOB_OUTCOMES = (
    "event.action: (job_completed or job_failed or job_dropped or job_deduplicated "
    "or job_redelivered or job_cancelled or job_empty)"
)

# (id, saved-object, grid x, y, w, h)
PANELS: List[Dict[str, Any]] = [
    # --- headline numbers
    {"id": "m-done", "x": 0, "y": 0, "w": 8, "h": 6, "obj": metric("Jobs completed", "event.action: job_completed")},
    {"id": "m-fail", "x": 8, "y": 0, "w": 8, "h": 6, "obj": metric("Jobs failed", "event.action: job_failed")},
    {"id": "m-exec", "x": 16, "y": 0, "w": 8, "h": 6, "obj": metric("Executions finished", "event.action: execution_finished")},
    {"id": "m-exec-bad", "x": 24, "y": 0, "w": 8, "h": 6, "obj": metric("Executions failed", "event.action: execution_finished and state: failed")},
    {"id": "m-stall", "x": 32, "y": 0, "w": 8, "h": 6, "obj": metric("Stalls / lost leases", "event.action: (dispatch_stalled or lease_expired or plan_abandoned or jobs_missing)")},
    {"id": "m-err", "x": 40, "y": 0, "w": 8, "h": 6, "obj": metric("Errors", "log.level: error")},
    # --- job health
    {"id": "jobs-outcome", "x": 0, "y": 6, "w": 24, "h": 14, "obj": bars("Jobs by outcome", JOB_OUTCOMES, _col_count(), "event.action")},
    {"id": "jobs-dur", "x": 24, "y": 6, "w": 24, "h": 14, "obj": bars("Job duration p95 (ms)", "event.action: job_completed", _col_pct("duration_ms", 95), "pipeline_name", "line")},
    {"id": "jobs-records", "x": 0, "y": 20, "w": 24, "h": 14, "obj": bars("Records written", "event.action: job_completed", _col_sum("records_out"), "pipeline_name")},
    {"id": "jobs-errtype", "x": 24, "y": 20, "w": 24, "h": 14, "obj": table("Failed jobs by pipeline and error type", "event.action: job_failed", ["pipeline_name", "error_type"])},
    # --- execution tracking
    {"id": "exec-state", "x": 0, "y": 34, "w": 24, "h": 14, "obj": bars("Executions finished by state", "event.action: execution_finished", _col_count(), "state")},
    {"id": "exec-runs", "x": 24, "y": 34, "w": 24, "h": 14, "obj": table("Runs accepted per pipeline", "event.action: run_accepted", ["pipeline_name"])},
    {"id": "exec-trouble", "x": 0, "y": 48, "w": 24, "h": 14, "obj": bars("Dispatch trouble", "event.action: (dispatch_stalled or lease_expired or plan_abandoned or jobs_missing or driver_replaced or dispatch_db_error)", _col_count(), "event.action")},
    {"id": "exec-pipe", "x": 24, "y": 48, "w": 24, "h": 14, "obj": table("Executions finished by pipeline and state", "event.action: execution_finished", ["pipeline_name", "state"])},
    # --- errors and warnings
    {"id": "err-time", "x": 0, "y": 62, "w": 24, "h": 14, "obj": bars("Warnings and errors by service", "log.level: (warning or error)", _col_count(), "service.name")},
    {"id": "err-events", "x": 24, "y": 62, "w": 24, "h": 14, "obj": table("Top warning / error events", "log.level: (warning or error)", ["service.name", "event.action", "log.level"])},
    {"id": "err-search", "x": 0, "y": 76, "w": 48, "h": 16, "search": True},
    # --- per pod: which replica did the work, and which one is failing
    {"id": "pod-jobs", "x": 0, "y": 92, "w": 24, "h": 14, "obj": bars("Jobs by pod", "event.action: (job_completed or job_failed)", _col_count(), "kubernetes.pod.name")},
    {"id": "pod-errors", "x": 24, "y": 92, "w": 24, "h": 14, "obj": table("Warnings and errors by pod", "log.level: (warning or error)", ["kubernetes.pod.name", "service.name", "event.action"])},
    # --- schedulers and DLQ
    {"id": "sched", "x": 0, "y": 106, "w": 24, "h": 14, "obj": bars("Schedule triggers", "event.action: (schedule_trigger or schedule_dispatched or schedule_trigger_error or schedule_dispatch_error)", _col_count(), "schedule_name")},
    {"id": "dlq", "x": 24, "y": 106, "w": 24, "h": 14, "obj": bars("DLQ activity", "event.action: (dlq_scheduled or dlq_due or dlq_dispatched or dlq_rescheduled or dlq_archived or dlq_cancelled or dlq_dispatch_error)", _col_count(), "event.action")},
]


def build() -> List[Dict[str, Any]]:
    objects: List[Dict[str, Any]] = [
        {
            "type": "index-pattern",
            "id": DATA_VIEW,
            "attributes": {"title": "reflowfy-logs-*", "timeFieldName": "@timestamp", "name": "Reflowfy logs"},
            "references": [],
            "coreMigrationVersion": "8.8.0",
            "typeMigrationVersion": "8.0.0",
        },
        {
            "type": "search",
            "id": "reflowfy-errors",
            "attributes": {
                "title": "Reflowfy errors",
                "columns": ["service.name", "kubernetes.pod.name", "event.action", "execution_id", "job_id", "message", "error.type"],
                "sort": [["@timestamp", "desc"]],
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(
                        {
                            "query": {"query": "log.level: error", KQL: "kuery"},
                            "filter": [],
                            "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                        }
                    )
                },
            },
            "references": [
                {"type": "index-pattern", "id": DATA_VIEW, "name": "kibanaSavedObjectMeta.searchSourceJSON.index"}
            ],
            "coreMigrationVersion": "8.8.0",
            "typeMigrationVersion": "8.0.0",
        },
    ]
    panels: List[Dict[str, Any]] = []
    refs: List[Dict[str, str]] = []
    for p in PANELS:
        pid = p["id"]
        if p.get("search"):
            ptype, oid = "search", "reflowfy-errors"
        else:
            ptype, oid = "lens", f"reflowfy-{pid}"
            objects.append({**p["obj"], "id": oid})
        panels.append(
            {
                "version": "8.11.0",
                "type": ptype,
                "gridData": {"x": p["x"], "y": p["y"], "w": p["w"], "h": p["h"], "i": pid},
                "panelIndex": pid,
                "embeddableConfig": {"enhancements": {}},
                "panelRefName": f"panel_{pid}",
            }
        )
        refs.append({"name": f"{pid}:panel_{pid}", "type": ptype, "id": oid})
    objects.append(
        {
            "type": "dashboard",
            "id": "reflowfy-logs-dashboard",
            "attributes": {
                "title": "Reflowfy logs",
                "description": "Jobs, executions, errors, schedulers and DLQ, from reflowfy-logs-*.",
                "panelsJSON": json.dumps(panels),
                "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "syncCursor": True, "syncTooltips": False, "hidePanelTitles": False}),
                "timeRestore": True,
                "timeFrom": "now-24h",
                "timeTo": "now",
                "refreshInterval": {"pause": False, "value": 30000},
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({"query": {"query": "", KQL: "kuery"}, "filter": []})
                },
            },
            "references": refs,
            "coreMigrationVersion": "8.8.0",
            "typeMigrationVersion": "8.9.0",
        }
    )
    return objects


def main() -> None:
    out = Path(__file__).with_name("kibana-saved-objects.ndjson")
    out.write_text("\n".join(json.dumps(o, separators=(",", ":")) for o in build()) + "\n")
    print(f"wrote {out} ({len(build())} objects)")


if __name__ == "__main__":
    main()
