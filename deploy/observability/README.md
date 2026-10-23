# Observability stack

`docker compose up` brings these up alongside the app:

| Service     | URL                     | Notes                                  |
|-------------|-------------------------|----------------------------------------|
| Prometheus  | http://localhost:9090   | Scrapes api/manager/worker `/metrics`  |
| Grafana     | http://localhost:3000   | Anonymous admin; **Reflowfy Overview** dashboard auto-loaded |
| APM Server  | http://localhost:8200   | OTLP → Elasticsearch (traces); enable by setting `OTEL_EXPORTER_OTLP_ENDPOINT` |

## Grafana

The dashboard in `grafana/dashboards/reflowfy-overview.json` is provisioned from
`grafana/provisioning/`. Edit the JSON and restart Grafana to update, or edit in
the UI and re-export.

## Kibana (logs + APM)

Logs land in `reflowfy-logs-*` when `LOG_DESTINATION` includes `elastic`. Create
a data view for `reflowfy-logs-*` in Kibana to explore them; APM views appear
automatically once APM Server ingests traces.

### Logs dashboard

`kibana/` holds a ready-made **Reflowfy logs** dashboard (Kibana 8.11+):

1. **Mappings, before logs arrive** (an existing daily index keeps its old mapping):
   `curl -X PUT "$ES/_index_template/reflowfy-logs" -H 'Content-Type: application/json' -d @kibana/index-template.json`
2. **Import**: Stack Management → Saved Objects → Import → `kibana/kibana-saved-objects.ndjson`
   (creates the `reflowfy-logs-*` data view, an errors saved search and the dashboard).
3. Open **Dashboards → Reflowfy logs**. To narrow it, type e.g. `pipeline_name: my_pipeline`
   or `execution_id: abc` in the query bar; every panel filters on those fields.

Sections: headline counts; per-pod jobs and errors (`kubernetes.pod.name`, so you can see which of several workers is failing); job health (outcomes, p95 duration, records written, failures by
error type); execution tracking (finished by state, runs accepted, stalls/lost leases);
warnings and errors by service and event, plus the error stream; schedulers and DLQ.

The import file is generated: edit `kibana/build_dashboard.py` and run
`python deploy/observability/kibana/build_dashboard.py`. The events and fields it uses are
listed in `docs/logging.md`.

See `../../docs/observability.md` for the full configuration reference.
