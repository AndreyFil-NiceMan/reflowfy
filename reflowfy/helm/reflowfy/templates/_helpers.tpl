{{/*
Expand the name of the chart.
*/}}
{{- define "reflowfy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "reflowfy.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "reflowfy.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "reflowfy.labels" -}}
helm.sh/chart: {{ include "reflowfy.chart" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: reflowfy
{{- end }}

{{/*
PostgreSQL host - returns internal service name if deployed, external host otherwise
*/}}
{{- define "reflowfy.postgresql.host" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "%s-postgresql" .Release.Name }}
{{- else }}
{{- .Values.postgresql.external.host }}
{{- end }}
{{- end }}

{{/*
PostgreSQL port
*/}}
{{- define "reflowfy.postgresql.port" -}}
{{- if .Values.postgresql.enabled }}
{{- 5432 }}
{{- else }}
{{- .Values.postgresql.external.port | default 5432 }}
{{- end }}
{{- end }}

{{/*
PostgreSQL database
*/}}
{{- define "reflowfy.postgresql.database" -}}
{{- if .Values.postgresql.enabled }}
{{- .Values.postgresql.auth.database }}
{{- else }}
{{- .Values.postgresql.external.database | default "reflowfy" }}
{{- end }}
{{- end }}

{{/*
PostgreSQL username
*/}}
{{- define "reflowfy.postgresql.username" -}}
{{- if .Values.postgresql.enabled }}
{{- .Values.postgresql.auth.username }}
{{- else }}
{{- .Values.postgresql.external.username | default "reflowfy" }}
{{- end }}
{{- end }}

{{/*
PostgreSQL password secret name
*/}}
{{- define "reflowfy.postgresql.secretName" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "%s-postgresql" .Release.Name }}
{{- else if .Values.postgresql.external.existingSecret }}
{{- .Values.postgresql.external.existingSecret }}
{{- else }}
{{- printf "%s-postgresql-external" .Release.Name }}
{{- end }}
{{- end }}

{{/*
PostgreSQL password secret key
*/}}
{{- define "reflowfy.postgresql.secretKey" -}}
{{- if .Values.postgresql.enabled -}}
password
{{- else if .Values.postgresql.external.existingSecret -}}
{{- .Values.postgresql.external.existingSecretPasswordKey | default "password" -}}
{{- else -}}
password
{{- end -}}
{{- end }}

{{/*
Database URL (with password placeholder for envsubst or direct injection)
*/}}
{{- define "reflowfy.databaseUrl" -}}
postgresql://{{ include "reflowfy.postgresql.username" . }}:$(DATABASE_PASSWORD)@{{ include "reflowfy.postgresql.host" . }}:{{ include "reflowfy.postgresql.port" . }}/{{ include "reflowfy.postgresql.database" . }}
{{- end }}

{{/*
Kafka bootstrap servers - returns internal service name if deployed, external servers otherwise
*/}}
{{- define "reflowfy.kafka.bootstrapServers" -}}
{{- if .Values.kafka.enabled }}
{{- printf "%s-kafka:9092" .Release.Name }}
{{- else }}
{{- .Values.kafka.external.bootstrapServers }}
{{- end }}
{{- end }}

{{/*
Kafka topic
*/}}
{{- define "reflowfy.kafka.topic" -}}
{{- .Values.kafka.topic | default "reflow.jobs" }}
{{- end }}

{{/*
Kafka group ID
*/}}
{{- define "reflowfy.kafka.groupId" -}}
{{- .Values.kafka.groupId | default "reflowfy-workers" }}
{{- end }}

{{/*
Kafka SASL secret name - external secret if provided, otherwise chart-generated
*/}}
{{- define "reflowfy.kafka.secretName" -}}
{{- if .Values.kafka.sasl.existingSecret }}
{{- .Values.kafka.sasl.existingSecret }}
{{- else }}
{{- printf "%s-kafka-sasl" .Release.Name }}
{{- end }}
{{- end }}

{{/*
Kafka SASL secret username key
*/}}
{{- define "reflowfy.kafka.usernameKey" -}}
{{- .Values.kafka.sasl.existingSecretUsernameKey | default "username" }}
{{- end }}

{{/*
Kafka SASL secret password key
*/}}
{{- define "reflowfy.kafka.passwordKey" -}}
{{- .Values.kafka.sasl.existingSecretPasswordKey | default "password" }}
{{- end }}

{{/*
Map the app SASL mechanism to the value KEDA's Kafka scaler expects
*/}}
{{- define "reflowfy.kafka.kedaSaslMechanism" -}}
{{- $m := .Values.kafka.sasl.mechanism | upper -}}
{{- if eq $m "SCRAM-SHA-256" -}}scram_sha256
{{- else if eq $m "SCRAM-SHA-512" -}}scram_sha512
{{- else if eq $m "PLAIN" -}}plaintext
{{- else -}}plaintext
{{- end -}}
{{- end }}

{{/*
KEDA TLS flag derived from the SASL security protocol
*/}}
{{- define "reflowfy.kafka.kedaTls" -}}
{{- if contains "SSL" (.Values.kafka.sasl.securityProtocol | upper) -}}enable
{{- else -}}disable
{{- end -}}
{{- end }}

{{/*
SASL environment variables shared by reflow-manager and worker deployments.
Username and password are read from the Kafka SASL secret; protocol and mechanism
are plain config values.
*/}}
{{- define "reflowfy.kafka.saslEnv" -}}
- name: KAFKA_SECURITY_PROTOCOL
  value: {{ .Values.kafka.sasl.securityProtocol | quote }}
- name: KAFKA_SASL_MECHANISM
  value: {{ .Values.kafka.sasl.mechanism | quote }}
- name: KAFKA_SASL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "reflowfy.kafka.secretName" . }}
      key: {{ include "reflowfy.kafka.usernameKey" . }}
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "reflowfy.kafka.secretName" . }}
      key: {{ include "reflowfy.kafka.passwordKey" . }}
{{- end }}

{{/*
Observability env: environment tag, log destination/shipping (Elastic), and traces.
Emitted into every service so production ships to Elastic and is filterable by
service.environment in Kibana. The ES password comes from a Secret, never inline.
*/}}
{{- define "reflowfy.observability.env" -}}
- name: ENVIRONMENT
  value: {{ .Values.observability.environment | default "production" | quote }}
- name: LOG_DESTINATION
  value: {{ .Values.observability.logDestination | default "stdout" | quote }}
- name: LOG_JSON
  value: {{ .Values.observability.logJson | default true | quote }}
{{- with .Values.observability.elasticLog }}
{{- if .url }}
- name: ELASTIC_LOG_URL
  value: {{ .url | quote }}
- name: ELASTIC_LOG_INDEX
  value: {{ .index | default "reflowfy-logs" | quote }}
{{- if .username }}
- name: ELASTIC_LOG_USERNAME
  value: {{ .username | quote }}
{{- end }}
{{- if or .password .existingSecret }}
- name: ELASTIC_LOG_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .existingSecret | default (printf "%s-elastic-log" $.Release.Name) }}
      key: {{ .existingSecretPasswordKey | default "password" }}
{{- end }}
{{- end }}
{{- end }}
{{- with .Values.observability.otel }}
{{- if .endpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .endpoint | quote }}
- name: OTEL_TRACES_SAMPLER
  value: "traceidratio"
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ .tracesSamplerArg | default "0.1" | quote }}
{{- end }}
{{- end }}
{{- end -}}
