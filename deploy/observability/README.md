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

See `../../docs/observability.md` for the full configuration reference.
